#!/bin/bash
#SBATCH --account=bioinfo_serv
#SBATCH --partition=bioinfo
#SBATCH --job-name=MergeProjChr   #job name
#SBATCH --mail-type=NONE # Mail events (NONE, BEGIN, END, FAIL, ALL)
##SBATCH --mail-user=marinaarroyomarta@gmail.com # Where to send mail
#SBATCH --mem-per-cpu=3gb # Per processor memory
#SBATCH --cpus-per-task=1
#SBATCH -t 15:00:00     # Walltime
#SBATCH -o /home/proyectos/bioinfo/fjd/maf_fjd/std/MergeProjChr_%j.out # Name output file
#SBATCH -e /home/proyectos/bioinfo/fjd/maf_fjd/std/MergeProjChr_%j.err
##SBATCH --file=
##SBATCH --initaldir=


#sleep 1000
#echo "hola" > $3
proj1=$1
proj2=$2
proj1_proj2=$3
#
NF=`awk -F"\t" '{print NF;exit}' $proj1`
#
cat $proj1 $proj2 | sort -T /scratch/ionut/sortMarta -k2,2n | /home/proyectos/bioinfo/fjd/maf_fjd/newParser/datamash-1.3/datamash -g1,2 sum 3-$NF > $proj1_proj2
