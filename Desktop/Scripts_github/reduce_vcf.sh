folder=$1
sample=$2
chr=$3


if [ "$chr" = "NULL" ]
then
	/home/proyectos/bioinfo/software/bcftools-1.9/bcftools norm -m '-' $folder"/"$sample"_raw.vcf" -o $folder"/biallelic_"$sample".vcf"

	/home/proyectos/bioinfo/software/bcftools-1.9/bcftools query -f '%CHROM\t%POS\t%REF\t%ALT[\t%GT]\n' -i 'FILTER="PASS" & FMT/DP>=10' $folder"/biallelic_"$sample".vcf" -o $folder"/reduce_"$sample".vcf"

	rm $folder"/biallelic_"$sample".vcf"


else

	mychr='_'$chr

	/home/proyectos/bioinfo/software/bcftools-1.9/bcftools norm -t $chr -m '-' $folder"/"$sample"_raw.vcf" -o $folder"/biallelic_"$sample".vcf"$mychr

	/home/proyectos/bioinfo/software/bcftools-1.9/bcftools query -f '%CHROM\t%POS\t%REF\t%ALT[\t%GT]\n' -i 'FILTER="PASS" & FMT/DP>=10' $folder"/biallelic_"$sample".vcf"$mychr -o $folder"/reduce_"$sample".vcf"$mychr

	rm  $folder"/biallelic_"$sample".vcf"$mychr

fi
