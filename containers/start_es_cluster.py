#!/usr/bin/env python3 

"""Stateful SLURM/MPI based ES cluster generator.
Tested with Python 3.10.12
Only works on Linux.
Expects one elasticsearch process per node.

High Level Description of how the startup works

We have a cluster with:
  - 1 master node iff N <= 3
  - 3 master nodes iff N > 3
All master nodes are data nodes.

First, we MPI GATHER together a list of tuples with
(rank, hostname)
to the root node (rank 0)
Note that we expect DNS/`/etc/hosts`-based name to IP resolution.

After that, for each of the nodes, we create its own elasticsearch folder.
In that folder, we will then edit the config.
See the Dockerfile why we do not include the elasticsearch into the container.

After that, we let the root process create a config at
{TEMP_DIRECTORY}/{RANK}/
for the process at rank {RANK}
All other processes wait at a barrier until the root is done, breaking the barrier.
The root nodes will always be
  - Rank 0 iff N <= 3
  - Rank 0,1,2 iff N > 3

Note that, if possible, we try to reuse the {RANK} folders from previous runs in order
to not require a re-ingest every start. This is possible iff the cluster size stays the same.

Lastly, the processes start their own elasticsearch container.

For a code overview, start at the `main()`. It was explicitly designed to be very expressive
"""

# Imports
import glob
import os
import shutil
import socket
import subprocess
import sys

from mpi4py import MPI

# Constants
ELASTICSEARCH_TAR_PATH = "./assets/elasticsearch-8.11.0-linux-x86_64.tar.gz"
ELASTICSEARCH_EXTRACTION_PATH = os.path.join(
    os.path.dirname(ELASTICSEARCH_TAR_PATH),
    "es"
)
DATA_PATH = "./es_data"
SINGULARITY_PATH = "./build/singularity_.sif"
USE_INFINIBAND = False # Only works on GWDG SCC cluster (Maybe OPA instead of IB)

COMM = MPI.COMM_WORLD

# Functions
def can_we_reuse_previous_es_data(data_path: str):
    """
    We want to avoid the ingest if possible since it takes a lot of time to
    ingest those large indices. 

    So instead, we try to reuse the previously generated elasticsearch folders.
    In order for this to work, we need an equivalently sized cluster folder.

    This function checks that the given data path contains the correct number
    of ES bind-mounts.

    This means that we just want folders of name `1`, `2`, to `n-1` where
    `n` is the WORLD-size. Note that we do not allow any other folder, since
    that may implies a bigger previous cluster size, i.e. downsizing would result
    in data loss.
    """
    n = COMM.Get_size()

    # Check if the path is a folder
    if not os.path.isdir(data_path):
        print("Previous ES data does not exist. Creating a new one", file=sys.stderr)
        return False

    expected_folders = {str(i) for i in range(n)}

    for item in os.listdir(data_path):
        item_path = os.path.join(data_path, item)

        # Is it the folder smaller than our new cluster size?
        if item not in expected_folders:
            print(f"Regenerating ES data. Reason: Folder \"{item_path}\" seems to indicate previously higher cluster size.", file=sys.stderr)
            return False

        if not os.path.isdir(item_path):
            print(f"Regenerating ES data. Reason: Found \"{item_path}\" which is not a directory, thus manually changed data.", file=sys.stderr)
            return False

        expected_folders.remove(item)

    # If we have any non-resolved folders, this means that we have to regenerate it
    if len(expected_folders) != 0:
        print(f"Regenerating ES data. Reason: Folder \"{next(iter(expected_folders))}\" seems to indicate previously higher cluster size.", file=sys.stderr)
        return False

    # We can reuse it!
    return True

def preconditions_or_die():
    """
    We check the following preconditions
    - Singularity has to be installed and loaded in
    - Singularity container has to be built
    - Elasticsearch has to be downloaded
    """
    if shutil.which("singularity") is None:
        print(f"Singularity is not installed (or loaded)!", file=sys.stderr)
        sys.exit(1)
    if not os.path.isfile(SINGULARITY_PATH):
        print(f"{SINGULARITY_PATH} does not exist!", file=sys.stderr)
        print("Please build the singularity container first.", file=sys.stderr)
        sys.exit(1)
    if not os.path.isfile(ELASTICSEARCH_TAR_PATH):
        print(f"{ELASTICSEARCH_TAR_PATH} does not exist!", file=sys.stderr)
        print("Please provide a valid archive containing elasticsearch", 
              file=sys.stderr)
        sys.exit(1)

def prepare(can_reuse: bool):
    """
    Does preparation work:
    - creates the temp path where the elasticsearch installations
      will be located
      (because they probably had different hostnames before)
    - extracts the ELASTICSEARCH_TAR_PATH tar.gz
    """
    # If we can't reuse the data, recreate it
    os.makedirs(DATA_PATH, exist_ok=True)
    if not can_reuse:
        for file in glob.glob(f"{DATA_PATH}/*"):
            shutil.rmtree(file)

    # extract the elasticsearch tar file
    print("Extracting elasticsearch, this can take some seconds...")
    base_folder, archive = os.path.split(ELASTICSEARCH_TAR_PATH)
    os.makedirs(ELASTICSEARCH_EXTRACTION_PATH, exist_ok=True)
    folder_name = os.path.basename(ELASTICSEARCH_EXTRACTION_PATH)
    subprocess.run(
        [
            "tar", 
            "xzf", 
            archive,
            "-C",
            f"./{folder_name}",
            "--strip-components=1"
        ],
        cwd=base_folder,
        check=True
    )



def get_host():
    """
    Returns a tuple of (rank, hostname)
    """
    rank = COMM.Get_rank()
    hostname = socket.gethostname()
    if USE_INFINIBAND:
        hostname = f"{hostname}.ib.gwdg.cluster"
    return rank, hostname

def create_es_folders():
    rank = COMM.Get_rank()
    dst_path = f"{DATA_PATH}/{rank}/"

    shutil.copytree(
        ELASTICSEARCH_EXTRACTION_PATH,
        dst_path,
        symlinks=True
    )
    

def gather_all_hosts():
    data = get_host()
    data = COMM.gather(data, root=0)
    return data

def all_hosts_are_different_or_die(hosts):
    if is_root():
        hostnames = [x[1] for x in hosts]
        unique_hostnames = [*set(hostnames)]
        all_hosts_different = len(hostnames) != len(unique_hostnames)
    else:
        all_hosts_different = False

    all_hosts_different = COMM.bcast(all_hosts_different, root=0)

    if all_hosts_different and is_root():
        print("This tool only supports one ES instance per node", file=sys.stderr)
    if all_hosts_different:
        sys.exit(3)

def is_root():
    """
    Checks whether the current node is the MPI root (i.e. rank 0)
    """
    return COMM.Get_rank() == 0

def create_all_configs(all_hosts):
    """
    We want a config that somewhat looks like

    ```
    cluster.name: securemetadata
    node.name: securemetadata<RANK_NUMBER>
    node.roles: ["master", "data"]
    network.host: 0.0.0.0
    cluster.initial_master_nodes: [securemetadata0]
    discovery.seed_hosts: ["rank0", "rank1", "rank2"]
    xpack.security.enabled: false
    ```
    """
    all_hosts.sort(key=lambda x: x[0])
    for i in range(len(all_hosts)):
        assert all_hosts[i][0] == i

    if len(all_hosts) > 3:
        discovery_seed_hosts = all_hosts[:3]
    else:
        discovery_seed_hosts = all_hosts[:1]

    big_cluster = len(all_hosts) > 3

    for (rank, hostname) in all_hosts:
        # Create config as shown in example above
        cluster_name = "securemetadata"
        node_name = f"securemetadata{rank}"
        if (big_cluster and rank <= 3) or rank == 0:
            node_roles = "[\"master\", \"data\"]"
        else:
            node_roles = "[\"data\"]"
        network_host = "0.0.0.0"
        cluster_initial_master_nodes = "[securemetadata0]"
        if len(all_hosts) > 3:
            discovery_seed_hosts = \
                f"[\"{all_hosts[0][1]}\", \"{all_hosts[1][1]}\", \"{all_hosts[2][1]}\"]"
        else:
            discovery_seed_hosts = f"[\"{all_hosts[0][1]}\"]"
        xpack_security_enabled = "false"

        # Write config to folder
        # Ugly, but most maintainable
        dst_path = f"{DATA_PATH}/{rank}/config/elasticsearch.yml"
        with open(dst_path, 'w') as fp:
            fp.write(f"cluster.name: {cluster_name}\n")
            fp.write(f"node.name: {node_name}\n")
            fp.write(f"node.roles: {node_roles}\n")
            fp.write(f"network.host: {network_host}\n")
            fp.write(f"cluster.initial_master_nodes: {cluster_initial_master_nodes}\n")
            fp.write(f"discovery.seed_hosts: {discovery_seed_hosts}\n")
            fp.write(f"xpack.security.enabled: {xpack_security_enabled}\n")

def update_all_configs(all_hosts):
    """
    See `can_we_reuse_previous_es_data` for why we do this and `create_all_configs` on
    how the configs are initially generated. We expect a config like

    ```
    cluster.name: securemetadata
    node.name: securemetadata<RANK_NUMBER>
    node.roles: ["master", "data"]
    network.host: 0.0.0.0
    cluster.initial_master_nodes: [securemetadata0]
    discovery.seed_hosts: ["rank0", "rank1", "rank2"]
    xpack.security.enabled: false
    ```
    
    and want to change out the underlying hosts while making sure that the indices still
    work when booting up the cluster. Thus, we only change the hostnames since this is the
    equivalent of only changing the IPs while leaving the same hardware (we have hostname
    based IP lookup).

    Thus, we only have to change:
    ```
    discovery.seed_hosts: ["<hostname of rank 0>", "<hostname of rank 1>", "<hostname of rank 2>"]
    ```

    Note that, just like in the `create_all_configs`, iff there are less than 3 hosts,
    `discovery.seed_hosts` is just rank 0.
    """
    all_hosts.sort(key=lambda x: x[0])
    for i in range(len(all_hosts)):
        assert all_hosts[i][0] == i

    if len(all_hosts) > 3:
        discovery_seed_hosts = \
            f"[\"{all_hosts[0][1]}\", \"{all_hosts[1][1]}\", \"{all_hosts[2][1]}\"]"
    else:
        discovery_seed_hosts = f"[\"{all_hosts[0][1]}\"]"

    for (rank, _) in all_hosts:
        file_path = f"{DATA_PATH}/{rank}/config/elasticsearch.yml"

        with open(file_path, 'r') as fp:
            lines = fp.readlines()

        with open(file_path, 'w') as fp:
            for line in lines:
                if line.startswith('discovery.seed_hosts:'):
                    fp.write(f"discovery.seed_hosts: {discovery_seed_hosts}\n")
                else:
                    fp.write(line)


def run_elasticsearch():
    rank = COMM.Get_rank()
    args = [
        "singularity",
        "run",
        "--cleanenv",
        "--bind",
        f"{DATA_PATH}/{rank}:/es",
        SINGULARITY_PATH
    ]
    with subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as process:
        for line in process.stdout:
            print(line.decode('utf8').rstrip("\n"))


def main():
    if is_root():
        can_reuse = can_we_reuse_previous_es_data(DATA_PATH)
        preconditions_or_die()
        prepare(can_reuse)
    else:
        can_reuse = None
    COMM.bcast(can_reuse, root=0)

    if not can_reuse:
        create_es_folders()
    COMM.Barrier()

    all_hosts = gather_all_hosts()
    all_hosts_are_different_or_die(all_hosts)
    if is_root():
        if can_reuse:
            update_all_configs(all_hosts)
        else:
            create_all_configs(all_hosts)

    COMM.Barrier()

    print("Configs successfully generated... starting elasticsearch")
    run_elasticsearch()
    
if __name__ == "__main__":
    if is_root():
        print(f"COMM_SIZE: {COMM.Get_size()}")
    main()
