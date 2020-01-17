# -*- coding: utf-8 -*-

import pandas as pd
import csv
import numpy.core.defchararray as npd
import os.path
import shutil #Para eliminar directorio haplotype
import glob
import os
import datetime
import argparse
import subprocess
import sys
import re
import time


## FUNCTIONS

def intersection(list1, list2):
	list3 = [value for value in list1 if value in list2]
	return list3


def update_db_gvcf(mapped_sites, chunk, pato, file):


	if (mapped_sites[0], int(mapped_sites[1]), 0) in chunk.index:

		#print('Posicion mapeada')

		chunk.loc[(mapped_sites[0], int(mapped_sites[1]), 0),'GT_00'] += 1

		chunk.loc[(mapped_sites[0], int(mapped_sites[1]), 0),pato+'_GT_00'] += 1


	elif (mapped_sites[0], int(mapped_sites[1])) in chunk.index:

		#print('Posicion con variante')

		for i in chunk.loc[(mapped_sites[0], int(mapped_sites[1]), 0),'GT_00']:

			i = i + 1

		for i in chunk.loc[(mapped_sites[0], int(mapped_sites[1]), 0),pato+'_GT_00']:

			i = i + 1


	else:


		file.write("%s\t%s\t%s\thola-function\n" %(mapped_sites[0], mapped_sites[1], mapped_sites[3]))
	# 	write_line_db(mapped_sites)
	# 	#print('Posicion nueva')

	# 	chunk.loc[(mapped_sites[0], int(mapped_sites[1]), mapped_sites[3], 0),'GT_00'] = 1

	# 	chunk.loc[(mapped_sites[0], int(mapped_sites[1]), mapped_sites[3], 0),pato+'_GT_00'] = 1



def update_db(variants, chunk, pato):

	if (variants[0], int(variants[1]), variants[2], variants[3]) in chunk.index:

		print('Variante en base de datos')

		if GT == '1/1' or GT == '1|1':

			chunk.loc[(variants[0], int(variants[1]), variants[2], variants[3]),'GT_11'] += 1

			chunk.loc[(variants[0], int(variants[1]), variants[2], variants[3]),pato+'_GT_11'] += 1

		elif GT == '0/1' or GT == '0|1':

			chunk.loc[(variants[0], int(variants[1]), variants[2], variants[3]) ,'GT_01'] += 1

			chunk.loc[(variants[0], int(variants[1]), variants[2], variants[3]),pato+'_GT_01'] += 1


	elif (variants[0], int(variants[1]), variants[2],0) in chunk.index:

		print('Variante mapeada')

		if GT == '1/1' or GT == '1|1':

			GT_00 = chunk.loc[(variants[0], int(variants[1]), variants[2], 0),'GT_00']

			pato_00 = chunk.loc[(variants[0], int(variants[1]), variants[2], 0),pato+'_GT_00']

			chunk.loc[(variants[0], int(variants[1]), variants[2], variants[3]),'GT_11'] = 1

			chunk.loc[(variants[0], int(variants[1]), variants[2], variants[3]),pato+'_GT_11'] = 1

			chunk.loc[(variants[0], int(variants[1]), variants[2], variants[3]),pato+'_GT_00'] = pato_00

			chunk.loc[(variants[0], int(variants[1]), variants[2], variants[3]),'GT_00'] = GT_00

			chunk.drop((variants[0], int(variants[1]), variants[2], 0), axis = 'index', inplace = True) #Elimino posicion mapeada


		elif GT == '0/1' or GT == '0|1':

			GT_00 = chunk.loc[(variants[0], int(variants[1]), variants[2], 0),'GT_00']

			pato_00 = chunk.loc[(variants[0], int(variants[1]), variants[2], 0),pato+'_GT_00']

			chunk.loc[(variants[0], int(variants[1]), variants[2], variants[3]),'GT_01'] = 1

			chunk.loc[(variants[0], int(variants[1]), variants[2], variants[3]),pato+'_GT_01'] = 1

			chunk.loc[(variants[0], int(variants[1]), variants[2], variants[3]),pato+'_GT_00'] = pato_00

			chunk.loc[(variants[0], int(variants[1]), variants[2], variants[3]),'GT_00'] = GT_00

			chunk.drop((variants[0], int(variants[1]), variants[2], 0), axis = 'index', inplace = True) #Elimino posicion mapeada


	else:

		print('variante nueva')

		if GT == '1/1' or GT == '1|1':

			chunk.loc[(variants[0], int(variants[1]), variants[2], variants[3]),'GT_11'] = 1

			chunk.loc[(variants[0], int(variants[1]), variants[2], variants[3]),pato+'_GT_11'] = 1


		elif GT == '0/1' or GT == '0|1':

			chunk.loc[(variants[0], int(variants[1]), variants[2], variants[3]),'GT_01'] = 1

			chunk.loc[(variants[0], int(variants[1]), variants[2], variants[3]),pato+'_GT_01'] = 1




def write_line_db(mapped_sites, pato, file, header):

	new_line =  mapped_sites[0:2]+["0"]*(len(header)-2)
	new_line[header.index('GT_00')] = "1"
	new_line[header.index(pato+'_GT_00')] = "1"
	file.write('\t'.join(new_line))



def main():

	## ARGUMENTS

	parser = argparse.ArgumentParser(description="UPDATE DATABASE BY CHROMOSOME")
	parser.add_argument('-v', '--vcfFolder', help='\t\tvcf file', required=True)
	parser.add_argument('-g', '--gvcfFolder', help='\t\tg.vcf file', required=True)
	parser.add_argument('-c', '--chr', required=True)
	parser.add_argument('-p', '--partition', default=True)
	#parser.add_argument('-t', '--threads' , help='\t\tNumber of threads', required=False, default=1)

	args = parser.parse_args()


	mychr='NULL'
	chrflag=''
	if args.partition:
		mychr=os.path.basename(args.chr).split('.')[0]
		chrflag="_"+mychr


	# MOSTRAR FECHA
	date = datetime.datetime.now()
	date = date.strftime("%Y-%m-%d")
	print(date)


	# TIEMPO INICIO
	start_time = time.time()


	## PATHS BIOINFO
	#gvcf_path = '/mnt/genetica/marta.marina/marta.marina/Allele_frequency/haplotype_caller_gvcf_data/reduce_14-1814.g.vcf' 	# GVCF
	samples_analysed =  '/mnt/genetica/marta.marina/marta.marina/Allele_frequency/samples_analysed.txt'   					    # Muestras analizadas
	patologies = '/mnt/genetica/marta.marina/marta.marina/Allele_frequency/variantsDataset_nodespistaje.csv'				    # Patologias
	#db_path = '/mnt/genetica/marta.marina/marta.marina/Allele_frequency/'                				# Base de datos
	#vcf_path = '/mnt/genetica/marta.marina/marta.marina/Allele_frequency/vcf_data/'                    						# VCFs
	#chr_path = '/mnt/genetica/marta.marina/marta.marina/Allele_frequency/vcf_data/chrs.txt'
	#HCGVCFD = '/mnt/genetica/marta.marina/marta.marina/Allele_frequency/haplotype_caller_gvcf_data/'
	AF = '/mnt/genetica/marta.marina/marta.marina/Allele_frequency/'
	scripts="/home/marta.marina/genetica/lorena_test/newParser"
	#AF_DB = '/mnt/genetica/marta.marina/marta.marina/Allele_frequency/variants_DB'
	#AF="/home/marta.marina/genetica/lorena_test/test_chr"
	#AF_DB ='/home/marta.marina/genetica/lorena_test/test_chr/chr21.db.txt'


	### COMPROBAR SI LA MUESTRA YA SE HA ANALIZADO ##

	# Si ya se ha analizado, BORRAR GVCF y VCF ¿?

	GVCF_list = glob.glob(args.gvcfFolder+'/*.g.vcf')   # List of files in gvcf directory (full path)

	gvcf_sample_list = []

	for gvcf in GVCF_list:

		gvcf_sample = os.path.basename(gvcf).split('.')[0]

		gvcf_sample_list.append(gvcf_sample)


	VCF_list = glob.glob(args.vcfFolder+'/*.vcf')     # List of files in vcf directory (full path)

	vcf_sample_list = []

	for vcf in VCF_list:

		vcf_sample = os.path.basename(vcf).split('_raw.vcf')[0]

		vcf_sample_list.append(vcf_sample)

	samples = intersection(gvcf_sample_list, vcf_sample_list)

	samples_to_analyse = []

	with open(samples_analysed, 'r+') as f:

		mylines = f.readlines()
		print(mylines)
		myoldsamples = [element.split('\n')[0] for element in mylines]

		for i in samples:

			if i in myoldsamples:
				print('Analizada')
				#shutil.rmtree(dir_prueba)  # PARA BORRAR LOS GVCFs
				sys.exit()

			else:
				print('Nueva')
				print(i)
				samples_to_analyse.append(i)


	print('SAMPLES TO ANALYSE:', samples_to_analyse)



	## EXCEL MUESTRA-ENFERMEDAD

	with open(AF+'variantsDataset_nodespistaje.csv') as enfermedades:

			enfermedades_reader = csv.reader(enfermedades, delimiter = ',')

			headers = next(enfermedades_reader)

			enfermedades = {}

			for sample in enfermedades_reader:

				ADN = sample[3]

				ADN = ADN.replace('/','-')

				Categoria = sample[-3] # 	key =  categoria

				Subcategoria = sample[-2] # value = subcategoria

				enfermedades[ADN] = {}

				enfermedades[ADN] = Subcategoria


	patologias = ['Enfermedad oftalmologica','Atrofia/neuropatia optica','Distrofia corneal','Distrofias de retina', 'Malformacion ocular','Otras',
	'Por clasificar','Cáncer', 'Cardiopatia', 'Dermatológicas','Digestivo','Encefalopatias, DI, Epilepsias', 'Esterilidad',
	'Endocrinologicas', 'Hemato inmunologia',' Hipoacusias','Inflamatoria','Malformaciones y Sind polimalformativos','Metabolicas','Miopatias',
	'Nefropatias','Neurodegeneracion','Neuropatias perifericas','Prenatal']

	patos = ['oftal','AO','DC','dd','MO','otras','pc','cancer','cardio','derma','diges','ence_DI_epi', 'ester','endoc','hemato','hipoa','infla','malfo',
	'metab','miopa','nefro','neurod','neurop','prenat']

	pato_dic = dict(zip(patologias, patos))


	## COUNTERS

	last_variant = None
	h = 0 					 # header
	c = 0  				     # chunks
	last_position = []
	new_last_position = []



	### GET NEWER DATABASE ###

	# list_of_db = glob.glob(AF_DB+'*.txt')

	# if len(list_of_db) != 0:

	# 	older_DB = min(list_of_db, key = os.path.getctime)   # OLDER DB

	# 	newer_DB = max(list_of_db, key = os.path.getctime)   # NEWER DB

	# 	if len(list_of_db) == 1 and len(list_of_db) != 0:

	# 		newer_DB = older_DB

	# 	else:

	# 		os.remove(older_DB)

	# 		print(older_DB, 'eliminado')



	### CREATE NEW DATABASE

	# chrom = os.path.basename(args.chr).split('.')[0]

	# DB_name = AF_DB+date+'_'+chrom+'_AF.txt'

	# if os.path.isfile(DB_name):

	# 	print('DATABASE NAME ALREADY EXISTS')

	# 	expand = 1

	# 	while True:

	# 		expand += 1

	# 		new_db_name = DB_name.split(".txt")[0] + str(expand) + ".txt"

	# 		if os.path.isfile(new_db_name):

	# 			continue

	# 		else:

	# 			DB_name = new_db_name

	# 			print(new_db_name)

	# 			break



	chunksize = 10000  # Determinar chunksize


	for sample in samples_to_analyse:

		nn=0
		nn_update=0
		# PATHOLOGY

		sample_patologia = enfermedades[str(sample)]

		pato = pato_dic[sample_patologia]


		## REDUCE GVCF

		start_time_part = time.time()

		reduce_GVCF = scripts+'/reduce_gvcf.sh'

		print('CREANDO GVCF REDUCIDO...')

		subprocess.call(['bash',reduce_GVCF, args.gvcfFolder, sample, mychr])

		print('Tiempo total =', time.time() - start_time_part)
		print('GVCF REDUCIDO CREADO')


		# UPDATE DATABASE WITH GVCF

		with open(args.chr) as myfile:
			header=myfile.readline()

		database = pd.read_csv(args.chr, delimiter = '\t', chunksize = chunksize, index_col = ['#CHROM','POS','ALT'], iterator = True)


		print('ABRIENDO GVCF...')
		new_DB_gvcf = open(args.chr+"_tmp", 'w')
		new_DB_gvcf.write(header)


		#with open(args.gvcfFolder+'reduce_'+sample+'.g.vcf') as reduce_gvcf:

		inputgvcf = args.gvcfFolder+'/reduce_'+sample+'.g.vcf'+chrflag

		print(inputgvcf)

		with open(inputgvcf) as reduce_gvcf:

			gvcf_reader = csv.reader(reduce_gvcf, delimiter = '\t')

			print('Leyendo base de datos en chunks...')

			for chunk in database:

				chunk.sort_index(inplace = True)

				c += 1

				chrom = chunk.iloc[-1:].index[0][0]

				last_pos = chunk.iloc[-1:].index[0][1]

				print('Tiempo por chunk =', time.time() - start_time)

				print('CHUNK', c, ':', chrom, last_pos)


				if len(last_position) != 0:

					print('ULTIMA POSICION LEIDA:', last_position)

					for i in last_position[:]:

						#print(i)

						if int(i[1]) <= last_pos:

							update_db_gvcf(i, chunk, pato, new_DB_gvcf)
							nn_update+=1

							last_position.remove(i)

						else:

							new_last_position = last_position
							# Escribir chunk

							chunk.reset_index(inplace = True)

							chunk.sort_values(by = ['#CHROM','POS'], axis=0 ,inplace = True)

							db = chunk.to_csv(new_DB_gvcf, sep = '\t', index = False, header = False )

							# Pasar al siguiente chunk

							break

							# GO TO THE NEXT CHUNK

					if len(new_last_position) != 0:

						continue


				for positions in gvcf_reader: # PARA CADA VARIANTE EN EL GVCF

						#nn+=1

					#if positions[0] == chrom:

						#print('Siguiente pos:', positions[0],'-',int(positions[1]),chrom,'-',last_pos)

						if positions[2] == '.':

							nn+=1

							if (int(positions[1]) <= last_pos):

								update_db_gvcf(positions, chunk, pato, new_DB_gvcf)
								nn_update+=1

							else:

								last_position = [positions]

								break

						else:

							for pos in range(int(positions[1]),int(positions[2])+1):

								nn+=1

								line = [positions[0],int(pos),positions[2],positions[3]]

								if pos <= last_pos:

									update_db_gvcf(line, chunk, pato, new_DB_gvcf)
									nn_update+=1

								else:

									print('Pasar a next chunk:',line)

									last_position.append(line)


							if len(last_position) != 0:

								break



				chunk.reset_index(inplace = True)

				chunk.sort_values(by = ['#CHROM','POS'],axis=0,inplace = True)

				#print('Chunk ordenado:',chunk)

				db = chunk.to_csv(new_DB_gvcf, sep = '\t',index = False, header = False )



			for i in last_position[:]:

				nn_update+=1
				new_DB_gvcf.write("hola-afterloop\n")


			for positions in gvcf_reader: # PARA CADA VARIANTE EN EL GVCF


				if positions[2] == '.':

					new_DB_gvcf.write("hola-remaininglines\n")

				else:

					for pos in range(int(positions[1]),int(positions[2])+1):

						nn+=1

						new_DB_gvcf.write("hola-remaininglines\n")


		new_DB_gvcf.close()

		print(nn)
		print(nn_update)

		print('FIN gVCF for sample %s' %(sample))

		print('Tiempo en actualizar base de datos con gvcf =', time.time() - start_time)


		sys.exit()


		## REDUCE VCF

		start_time_part = time.time()

		reduce_VCF = scripts+'/vcf_extract_gt.sh'

		print('CREANDO VCF REDUCIDO...')

		subprocess.call(['bash',reduce_VCF, args.vcfFolder, sample,  mychr])

		print('Tiempo total =', time.time() - start_time_part)
		print('VCF REDUCIDO CREADO')


		## UPDATE DATABASE WITH VCF


		with open(args.chr+"_tmp") as myfile:
			header=myfile.readline()


		new_DB_gvcf = pd.read_csv(args.chr+"_tmp", chunksize = chunksize, delimiter = '\t', index_col = ['#CHROM','POS','REF','ALT'], iterator = True)


		print('ABRIENDO VCF...')
		new_DB = open(args.chr, 'w')
		new_DB.write(header)


		inputvcf = args.gvcfFolder+'reduce_'+sample+'.vcf'+chrflag
		with open(inputvcf) as reduce_vcf:

			vcf_reader = csv.reader(reduce_vcf, delimiter = '\t')

			for chunk in new_DB_gvcf:

				#print(chunk)

				chunk.sort_index(inplace = True)

				c += 1

				chrom = chunk.iloc[-1:].index[0][0]

				last_pos = chunk.iloc[-1:].index[0][1]

				#print('Tiempo por chunk =', time.time() - start_time)

				#print('CHUNK',c, ':', chrom, last_pos)


				if last_variant != None:

					#print('ULTIMA VARIANTE LEIDA:', last_variant)

					if int(last_variant[1]) <= int(last_pos):

						GT = last_variant[4]

						update_db(last_variant, chunk, pato)

						last_variant = None

					else:

						# ESCRIBIR
						chunk.reset_index(inplace = True)

						chunk.sort_values(by = ['#CHROM','POS'], axis=0 ,inplace = True)

						db = chunk.to_csv(new_DB, sep = '\t', index = False, header = False)

						# GO TO THE NEXT CHUNK

						continue


				for variants in vcf_reader: # PARA CADA VARIANTE EN EL VCF

					if variants[0] == chrom:

						#print(variants)

						print('Siguiente variante:',variants[0],'-', int(variants[1]), chrom,'-', last_pos)

						if int(variants[1]) <= last_pos:

							GT = variants[4]

							update_db(variants, chunk, pato)

						else:

							last_variant = variants

							break



				chunk.reset_index(inplace = True)

				chunk.sort_values(by = ['#CHROM','POS'],axis=0,inplace = True)

				#print('Chunk ordenado:',chunk)

				db = chunk.to_csv(new_DB, sep = '\t',index = False, header = False )

				#print(chunk)



		new_DB.close()
		print('FIN VCF for sample %s' %(sample))

		print('Tiempo en actualizar base de datos con vcf=', time.time() - start_time)




		# ADD SAMPLE INFO TO SAMPLE ANALYSED.

		with open(samples_analysed+"_tmp", 'w') as f:

			f.write(sample+'\n')




	## CALCULATE MAF WHEN ALL SAMPLES ANALYZED

	# new_DB = pd.read_csv(args.csv, chunksize = chunksize, delimiter = '\t')

	# for chunk in new_DB:

	# 	chunk['MAF'] = (chunk['GT_11'] + (chunk['GT_01']/2))/chunk['GT_00']

	# 	chunk['MAF_'+pato] = (chunk[pato+'_GT_11'] + (chunk[pato+'_GT_01']/2))/chunk[pato+'_GT_00']

	# new_DB.close()

	# os.remove(AF_DB+'new_DB_gvcf.txt')

	# print('Tiempo total =', time.time() - start_time)

	# # DELETE REDUCED GVCFs AND REDUCED VCFs

	# for sample in samples_to_analyse:

	# 	sample_vcf_path = args.vcfFolder+'reduce_'+sample+'.vcf'

	# 	sample_gvcf_path = args.gvcfFolder+'reduce_'+sample+'.g.vcf'

	# 	os.remove(sample_vcf_path)

	# 	os.remove(sample_gvcf_path)







if __name__ == '__main__':
    main()

