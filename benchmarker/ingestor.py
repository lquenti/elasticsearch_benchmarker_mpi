#!/usr/bin/env python3

"""Elasticsearch Ingest Benchmarker
Tested with Python 3.10.12
High level idea:
- Arrange
  - Root only
    - First iteration: count the number of lines (i.e. documents), compute which lines each rank has to do.
    - Second iteration: Find the byte offsets for each rank start, write them into an `.offset` file
    - Validate that, starting at the current byte, the line for each rank has a proper JSON doc
    - Root rank creates the Elasticsearch index
  - MPI Barrier
- Act
  - for each rank
    - Seek to the starting byte offset based on the rank
    - track starting time
    - blast through the requests blockingly, as fast as possible, track each req time
    - MPI barrier
    - Root gathers all data
- Assert
  - Root dumps everything in a json
"""

import argparse
import json
import os
import time

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import requests

from dataclasses_json import DataClassJsonMixin
from mpi4py import MPI

DEFAULT_BULK_SIZE = 1000
DEFAULT_INDEX_NAME = "benchmark_index"

COMM = MPI.COMM_WORLD

# for easier grepping through the code
DEBUG = True
def noop(*args, **kwargs):
    pass
dbg = print if DEBUG or os.environ.get("DEBUG") is not None else noop

@dataclass
class Offset(DataClassJsonMixin):
    rank: int
    starting_line: int
    starting_byte: int
    # If None, go till the end
    number_of_lines: Optional[int]

@dataclass
class Offsets(DataClassJsonMixin):
    number_of_workers: int
    offsets: list[Offset]

    def append(self, *args, **kwargs):
        self.offsets.append(*args, **kwargs)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Load ingestor for Elasticsearch.")

    parser.add_argument(
        "--data",
        type=str,
        help="Path to the JSON data to be ingested",
        required=True
    )
    parser.add_argument(
        "--index",
        type=str,
        help="Path to the Metadata Description to be ingested",
        required=True
    )
    parser.add_argument(
        "--hosts",
        type=str,
        help="Comma separated hosts. Example: \"--hosts host1:9200,10.0.0.2:9200,localhost:9200\"",
        required=True
    )
    parser.add_argument(
        "--bulksize",
        type=int,
        default=DEFAULT_BULK_SIZE,
        help=f"Number of documents sent per request (defaults to {DEFAULT_BULK_SIZE}) (Optional)"
    )
    parser.add_argument(
        "--indexname",
        type=str,
        default=DEFAULT_INDEX_NAME,
        help="Name of the elasticsearch index created for the specified data."
    )
    parser.add_argument(
        "--shards",
        type=int,
        help="Number of shards, defaults to 1 shard per host"
    )
    args = parser.parse_args()
    
    # Do the files exist
    if not os.path.isfile(args.data):
        parser.error(f"Data path \"{args.data}\" is not a valid file!")
    if not os.path.isfile(args.index):
        parser.error(f"Data path \"{args.index}\" is not a valid file!")

    # Are the files readable
    if not os.access(args.data, os.R_OK):
        parser.error(f"Data path \"{args.data}\" is not a readable!")
    if not os.access(args.index, os.R_OK):
        parser.error(f"Data path \"{args.data}\" is not a readable!")

    # Are the hosts well formatted, i.e. do they contain a port?
    all_hosts = [x.strip() for x in args.hosts.split(",") if x.strip() != ""]
    for host in all_hosts:
        colon_index = host.rfind(":")

        if colon_index == -1:
            parser.error(f"Host \"{host}\" does not contain a port!")

        port = host[colon_index+1:]
        if not port.isdigit() or int(port) > 65535:
            parser.error(f"Host \"{host}\" does not contain a valid port!")

    # Do we need a default for the number of shards?
    if args.shards is None:
        args.shards = sum([1 for x in args.hosts.split(",") if x.strip() != ""])

    return args

def is_root():
    return COMM.Get_rank() == 0

def calculate_offsets(args):
    # calculate file length
    n_docs = 0
    with open(args.data, 'r') as fp:
        for _ in fp:
            n_docs+=1

    # Yes, last worker has to do a bit more work, but realistically this
    # is 1 req more max with a bulk size of 1000.
    docs_per_worker = n_docs // COMM.Get_size()

    # Count for each rank to which byte they have to seek to
    offset_file = f"{args.data}.offset"

    # Check if we already have a valid computation
    offsets = None
    try:
        with open(offset_file, "r") as fp:
            offsets = Offsets.schema().loads(fp.read())

        # If it was created for another mpi comm size we unfortunatley cant use it
        if offsets.number_of_workers != COMM.Get_size():
            print("Offset was generated for different MPI size...")
            offsets = None
    except Exception as e:
        print("Loading the Offset file didn't work")
        print(f"Exception: {e}")
        offsets = None

    # Compute the offsets if not done already
    if offsets is None:
        offsets = Offsets(number_of_workers=COMM.Get_size(), offsets=[])

        current_rank = 0
        current_line = 0
        with open(args.data, "rb") as fp:
            while current_line < n_docs:
                # This could maybe? overflow due to the rounding in
                # docs_per_worker? We manage it anyways within the if
                if (current_line % docs_per_worker) == 0 and current_rank < COMM.Get_size():
                    offset = Offset(
                        rank=current_rank,
                        starting_line=current_line,
                        starting_byte=fp.tell(),
                        number_of_lines=docs_per_worker 
                            if (current_rank != COMM.Get_size()-1) else
                            None

                    )
                    offsets.append(offset)
                    current_rank += 1
                next_line = fp.readline()
                current_line += 1
                if not next_line:
                    break

    # Verify that the offsets result in valid json
    with open(args.data, "rb") as fp:
        for o in offsets.offsets:
            fp.seek(o.starting_byte)

            # verify that this is a starting byte for a json line
            line = fp.readline()
            try:
                json.loads(line)
            except Exception as e:
                print("Invalid JSON offsets!")
                print(f"Offset: {o}")
                raise e

    # if its valid, save it
    if os.path.isfile(offset_file):
        os.remove(offset_file)
    with open(offset_file, "w") as fp:
        json_str = Offsets.schema().dumps(offsets)
        fp.write(json_str)

    return offsets



def create_index(args):
    all_hosts = [x.strip() for x in args.hosts.split(",") if x.strip() != ""]
    url = f"http://{all_hosts[0]}/{args.indexname}"

    # just send it to the first host
    print("Dropping index if exists")
    requests.delete(url)

    print("Creating index")
    # first, load the index mappings
    with open(args.index, "r") as fp:
        index_mappings = json.load(fp)
    index_settings = {
        "settings": {
            "index": {
                "number_of_shards": args.shards,
                "requests.cache.enable": "false"
            }
        },
        "mappings": {
            "dynamic": "strict",
            "properties": index_mappings["properties"]
        }
    }
    requests.put(
        url,
        data=json.dumps(index_settings),
        headers={'Content-Type': 'application/json'},
    )

def bulk_fill_index(args, offsets: Offsets):
    all_hosts = [x.strip() for x in args.hosts.split(",") if x.strip() != ""]
    all_endpoints = [f"http://{host}/{args.indexname}/_bulk" for host in all_hosts]
    assert len(all_hosts) <= offsets.number_of_workers, "Expecting at least as many load gens as cluster instances"

    rank = COMM.Get_rank()
    index_str = '{"index": {"_index": "' + args.indexname + '"}}'

    times = []

    # We have to distribute the requests between all ES cluster nodes...
    # I'd expect people to understand this is somehow a problem (as mentioned in the README).
    # Thus, we assume that the MPI World Size is a multiple of the number of ES cluster nodes.
    # With that in mind, we do the following load distribution (assuming 3 ES nodes)
    #
    # rank 0 requests against ES 0
    # rank 1 requests against ES 1
    # rank 2 requests against ES 2
    # rank 3 requests against ES 0
    # rank 4 requests against ES 1
    # rank 5 requests against ES 2
    # rank 6 requests against ES 0
    # ...
    es_to_request_against = all_endpoints[rank % len(all_endpoints)]
    my_offset = offsets.offsets[rank]
    with open(args.data, "r") as fp:
        # seek to my start
        fp.seek(my_offset.starting_byte)

        bulk_data = []
        cnt = 0
        for line in fp:
            bulk_data.append(index_str)
            bulk_data.append(line)
            cnt += 1

            if cnt % args.bulksize == 0:
                before = datetime.now()
                requests.post(
                    es_to_request_against,
                    data="\n".join(bulk_data),
                    headers={'Content-Type': 'application/json'}
                )
                after = datetime.now()
                times.append(str(after-before))
                bulk_data = []

            if cnt == my_offset.number_of_lines:
                # we did our work
                break

        if not cnt % args.bulksize == 0:
            # send the rest
            before = datetime.now()
            requests.post(
                es_to_request_against,
                data="\n".join(bulk_data),
                headers={'Content-Type': 'application/json'}
            )
            after = datetime.now()
            times.append(str(after-before))

        print(f"Rank {COMM.Get_rank()} done!")

        return times

def dump_timestamps(times):
    times = [str(t) for t in times]
    timestamp = int(time.time())

    # We use the timestamp only so that each rank has a high likelyhood
    # of a unique name
    with open(f"./results_{timestamp}.txt", "w") as fp:
        fp.writelines(times)

def main():
    args = parse_arguments()
    if is_root():
        offsets = calculate_offsets(args)
        create_index(args)
    else:
        offsets = None
    COMM.Barrier()

    # Give offsets to everone
    offsets = COMM.bcast(offsets, root=0)
    COMM.Barrier()

    times = bulk_fill_index(args, offsets)
    COMM.Barrier()

    # Gather all data to root
    times = COMM.gather(times, root=0)
    
    if is_root():
        dump_timestamps(times)

if __name__ == "__main__":
    main()
