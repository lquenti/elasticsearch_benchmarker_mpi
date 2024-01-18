all: clean
	sudo ./build_singularity.sh ./Elasticsearch.Dockerfile

# sandbox: clean
# 	sudo ./build_sandbox.sh ./Elasticsearch.Dockerfile

clean:
	sudo rm -rf ./docker_.tar.gz ./singularity_.sif ./es_data ./build

.PHONY: all clean sandbox
