#!/bin/bash
#SBATCH --reservation=gzadmhno_2
#SBATCH --ntasks-per-node=1
#SBATCH -N 3

module load openmpi
module load intel
module load python/3.9.16
module load singularity

SCRIPT_DIR="/scratch-emmy/usr/nimlarsq/securemetadatav2/containers"

source ${SCRIPT_DIR}/venv/bin/activate
mpirun -n 3 python3 ${SCRIPT_DIR}/start_es_cluster.py
