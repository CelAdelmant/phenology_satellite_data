#!/bin/bash
#SBATCH --job-name=pheno-preprocessing
#SBATCH --output=./scripts/slurm-out/pheno-preprocessing.out
#SBATCH --error=./scripts/slurm-out/pheno-preprocessing.err
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=48
#SBATCH --time=00:30:00
#SBATCH --mem-per-cpu=1G
#SBATCH --partition=short

module purge
module load R/4.2

# programmatically go to the root of the repo,
# then run the preprocessing script
cd $(git rev-parse --show-toplevel)
Rscript ./scripts/preprocessing.R
