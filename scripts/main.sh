#!/bin/bash
#SBATCH --job-name=pheno-main
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=48
#SBATCH --time=03:00:00
#SBATCH --mem-per-cpu=1G
#SBATCH --partition=medium
#SBATCH --output=./scripts/slurm-out/main-%j.out
#SBATCH --error=./scripts/slurm-out/main-%j.err


module purge
module load R/4.2

# programmatically go to the root of the repo,
# then run the main script
cd $(git rev-parse --show-toplevel)
Rscript ./scripts/main.R

