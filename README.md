# MPI-based Creation and Benchmarking of a Dynamic Elasticsearch Cluster

## READ THE [REPORT](./report.pdf) OR THE PRESENTATION [SLIDES](./presentation.pdf)

## Structure
- [`benchmarker`](./benchmarker) contains
  - The benchmarking code for ingestion
  - The querying benchmarker
- [`containers`](./containers) contains
  - A workflow for building a singularity container with elasticsearch bindmounted into it
  - [A startup script](./containers/start_es_cluster.py) to build up an elasticsearch cluster
- [`mrirally`](./mrirally) contains the code for generating fake data
- [`latex.tgz`](./latex.tgz) contains the latex code for report, presentation as well as the analysis scripts with the benchmark data

## What has to be done beforehand!
- Download `nyc_taxis` documents.json (70GB+) and put it into `./benchmarker/data/nyc_taxis`
