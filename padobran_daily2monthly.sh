#!/bin/bash

#PBS -N daily2monthly
#PBS -l ncpus=1
#PBS -l mem=5GB
#PBS -J 1-8323
#PBS -o logs
#PBS -j oe

cd ${PBS_O_WORKDIR}

apptainer run image.sif padobran_daily2monthly.R