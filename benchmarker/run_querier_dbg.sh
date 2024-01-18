# the spawned elasticsearch config is made with a cluster in mind
# Thus, when only running on one node, we only get a yellow cluster health state
# due to one extra shard not allocated (since it expects redundancy yk)
# For that, we have an extra flag --allow-bad

# expects venv
source venv/bin/activate

N=4

# delete old result
rm -f res.json

time mpirun -n $N python3 queryer.py \
  --hosts 127.0.0.1:9200 \
  --output res.json \
  --warmup 0 \
  --execution 5 \
  --sleep 1 \
  --allowbad \
  data/nyc_taxis/query.json

