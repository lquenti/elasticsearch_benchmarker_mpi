SCRIPT_DIR="." # todo

${SCRIPT_DIR}/venv/bin/python3 ${SCRIPT_DIR}/ingestor.py --data ./data/nyc_taxis/documents-1k.json --index ./data/nyc_taxis/index.json --hosts localhost:9200 --bulksize 100 --shards 25
