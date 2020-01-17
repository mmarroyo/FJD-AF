#!/bin/bash

GTfile=$1
Nfile=$2
mergeFiles=$3


#sleep 100
#echo 'Hola' > $mergeFiles

join -j2 $GTfile $Nfile | awk '{printf $2"\t"$1"\t";for (i=3;i<=NF;i++) printf "%s",$i "\t"}{printf "\n"}' | cut -f -97,99- > $mergeFiles
