#!/bin/bash
#SBATCH --nodes=8
#SBATCH --partition=g40
#SBATCH --job-name=dedup_chinese
#SBATCH --output=/fsx/hyein/2023/dedup2.log
#SBATCH --error=/fsx/hyein/2023/dedup2.err
#SBATCH --comment eleuther

echo "start"
python bin/sparkapp.py dedup_job --config_path=./configs/dedup_job_ch.yaml
echo "end"                
