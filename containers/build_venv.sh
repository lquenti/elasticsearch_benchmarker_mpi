module load gcc/9.3.0
module load openmpi/gcc.9/4.1.4
#module load intel
module load python/3.9.16
module load singularity

# deactivate if exists
deactivate 2> /dev/null

DIR="/scratch-emmy/usr/nimlarsq/securemetadatav2/containers"
rm -rf ~/.cache
cd $DIR
rm -rf ${DIR}/venv
python3 -m venv venv
source ${DIR}/venv/bin/activate
echo "test: $(which python3)"
python3 -m pip install -r ${DIR}/requirements.txt
