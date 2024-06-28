#!/bin/bash
#SBATCH --job-name=jupyter
#SBATCH --output=jupyter_output.txt
#SBATCH --error=jupyter_error.txt
#SBATCH --time=05:00:00
#SBATCH --ntasks=1
#SBATCH --mem-per-cpu=9000
#SBATCH --mail-type=ALL
#SBATCH --mail-user=s.gao2@vu.nl


# Load necessary modules (adjust based on your environment)
module load python/3.10
module load jupyter

# Activate virtual environment
source /scistor/ivm/sga226/environment/bin/activate

# Start SSH tunnel for port forwarding
ssh -o StrictHostKeyChecking=no -f -N -R 8889:localhost:8889 login2.labs.vu.nl

# Start Jupyter Notebook
jupyter-lab --no-browser --port=8889