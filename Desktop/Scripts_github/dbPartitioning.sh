#!/bin/bash


db=$1
analysis=$2
db_updated=$3

echo $db
echo $analysis



# partition (at the beggining of the running)

if [ "$analysis" == "partitioning" ]; then

	#header=`zcat $db | head -n1`

	folder=$(dirname $db)

	echo  `tabix $db -l`
	for chr in `tabix $db -l`; do

		echo $chr
		zcat $db | head -n1 >  $folder"/"$chr".db.txt" 
		tabix $db $chr >>  $folder"/"$chr".db.txt" 

	done

fi



# merge (at the end of the running)


if [ "$analysis" == "merging" ]; then


	zcat $db | head -n1 > $db_updated
	folder=$(dirname $db)
	echo $folder

	for chr in `tabix $db -l`; do

		cat $folder"/"${chr}.db.txt_updated_AF >> $db_updated 

	done

	#awk 'NR<2{print $0; next}{print $0 | \"sort -k1,1 -k2,2n\" }' $db > $db_sorted
	bgzip $db_updated
	tabix -b 2 -e 2 -s 1  $db_updated.gz


	# actualizar samples.
	samples_analysed = '/home/proyectos/bioinfo/fjd/maf_fjd/others/samples_analysed.txt'
	samples_analysed_tmp = '/home/proyectos/bioinfo/fjd/maf_fjd/others/samples_analysed.txt_tmp'
	mv samples_analysed_tmp samples_analysed  


fi
