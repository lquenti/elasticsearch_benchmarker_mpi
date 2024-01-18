FROM ubuntu:22.04

# Usually, we would copy and extract elasticsearch here.
# but unfortunately, elasticsearch expects to be the owner of the files,
# and disables some neat stuff such as autoconfiguration whenever it isnt.
# Since singularity is rootless, we cant enfore the correct uid for the folders
# thus we just bind-mount the whole elasticsearch in...

# vim/curl are just for debugging
RUN apt-get update && apt-get install -y \
  vim \
  curl \
  && rm -rf /var/lib/apt/lists/*

# Debug: run forever
# CMD tail -f /dev/null

RUN mkdir /es

CMD /es/bin/elasticsearch
