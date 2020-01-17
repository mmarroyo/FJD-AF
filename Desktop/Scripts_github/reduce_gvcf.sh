#!/bin/bash

folder=$1

sample=$2

chr=$3


if [ "$chr" = "NULL" ]
then

	mychr=''
	/home/proyectos/bioinfo/software/bcftools-1.9/bcftools query -i "FMT/DP>=10" -f "%CHROM\t%POS[\t%INFO/END]\t%REF\n" $folder"/"$sample".g.vcf" -o $folder"/reduce_"$sample".g.vcf"

else

	mychr='_'$chr
	/home/proyectos/bioinfo/software/bcftools-1.9/bcftools query -t $chr -i "FMT/DP>=10" -f "%CHROM\t%POS[\t%INFO/END]\t%REF\n"  $folder"/"$sample".g.vcf"  -o  $folder"/reduce_"$sample".g.vcf"$mychr


fi
