# Code for "MPI-based Creation and Benchmarking of a Dynamic Elasticsearch Cluster"

WIP report (some benchmarks missing), ETA late September 2024

## Structure
- [`benchmarker`](./benchmarker) contains
  - The benchmarking code for ingestion
  - The querying benchmarker (in progress)
- [`containers`](./containers) contains
  - A workflow for building a singularity container with elasticsearch bindmounted into it
  - [A startup script](./containers/start_es_cluster.py) to build up an elasticsearch cluster
- [`mrirally`](./mrirally) contains the code for generating fake data

## What has to be done beforehand!
- Download `nyc_taxis` documents.json (70GB+) and put it into `./benchmarker/data/nyc_taxis`
