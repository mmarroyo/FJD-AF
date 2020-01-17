!/bin/bash
### Script for running each chr in a batch.


import argparse
import glob
import os
import sys
import subprocess


def sbatch(job_name, folder_out, command, mem=5, time=4000000, threads=3, mail=None, dep='', wait = ''):

	if dep != '':
		dep = '--dependency=afterok:{} --kill-on-invalid-dep=yes '.format(dep)

	if mail!=None:
		mailc = "--mail-user={} --mail-type=FAIL".format(mail)
	else:
		mailc = ''

	if wait==True:
		wait = '--wait'


	sbatch_command = "sbatch -J {} -o {}/{}.out -e {}/{}.err {} -t {}:00:00 --account=bioinfo_serv --partition=bioinfo --mem-per-cpu={}gb --cpus-per-task={} {} {} {}".format(job_name, folder_out, job_name, folder_out, job_name, mailc, time, mem, threads, dep, wait, command)
	print(sbatch_command)

	sbatch_response = subprocess.check_output(sbatch_command, shell=True)
	job_id = sbatch_response.split(' ')[-1].strip()
	return job_id
			


def main():

	#arguments
	parser = argparse.ArgumentParser(description="UPDATE MAF DATABASE")
	#parser.add_argument('-p', '--projectFolder', help='\t\tproject folder', required=True)
	parser.add_argument('-v', '--vcfFolder', help='\t\tvcf folder', required=True)
	parser.add_argument('-g', '--gvcfFolder', help='\t\tg.vcf folder', required=True)
	parser.add_argument('-o', '--output', help='\t\toutput folder', required=True)
	parser.add_argument('-n', '--name', help='\t\tproject name', required=True)
	
	#parser.add_argument('-t', '--threads' , help='\t\tNumber of threads', required=False, default=1)

	args = parser.parse_args()
	args.mail="marinaarroyomarta@gmail.com"



	### DEFINE LAST DATABASE, THAT IS THE ONE THAT WILL BE UPDATED

	dbFolder = args.output
	
	#list_of_db = glob.glob('/mnt/genetica/marta.marina/marta.marina/Allele_frequency/variants_DB/*.txt') 

	#if len(list_of_db) != 0:
		
	#	older_DB = min(list_of_db, key = os.path.getctime)   # OLDER DB
		
	#	newer_DB = max(list_of_db, key = os.path.getctime)   # NEWER DB
		
	#	if len(list_of_db) == 1 and len(list_of_db) != 0:
		
	#		newer_DB = older_DB
		
	#	else:
			
	#		os.remove(older_DB)
			
	#		print(older_DB, 'eliminado')

	# define db version
	dbFile = dbFolder+"/maf_empty.txt.gz" # CON PATH!!!!!!
	print(dbFile)
	dbUpdate = dbFolder+"/maf_"+args.name+"_updated.txt"		
	print(dbUpdate)

	### 1. Divide database
	#jobidpartlist = []
	
	
	#print("\nDividing database into individual chromosomes")
	#myargs = ["/home/proyectos/bioinfo/fjd/maf_fjd/newParser/dbPartitioning.sh", dbFile, "partitioning"]
	#job_name =  "dbPartitioning"
	#print("JOB NAME: %s\n" %(job_name))
	#sbatch(job_name, args.output, ' '.join(myargs), mail=args.mail, dep='', wait=True)
	#jobidpart = sbatch(job_name, args.ou, ' '.join(myargs), mail=args.mail, dep='', wait=True)
	#jobidpart = sbatch(job_name, args.output, ' '.join(myargs), mail=args.mail, dep='')
	#jobidpartlist.append(jobidpart)

	### 2. Run for each chr

	jobidlist = []

	for chr_file in glob.glob(dbFolder+'/*.db.txt'):

		print("\nUpdating each individual chromosome:", chr_file)
		myargs = ["python","/home/proyectos/bioinfo/fjd/maf_fjd/newParser/update_db_chunks_chr_args_bioinfo.py","-c",chr_file,"-g",args.gvcfFolder,"-v",args.vcfFolder]   ###### CAMBIAR 
		job_name = "chr_updatabase"

		
		job_file = "slurm_concat.sh"
		with open(job_file, "w") as fh:
			fh.writelines("#!/bin/bash\n%s\n%s\n" %('module load python/gnu/4.4.7/2.7.11',' '.join(myargs)))
		jobidchr = sbatch(job_name, args.output, job_file, mail=args.mail, dep='')
		jobidlist.append(jobidchr)
		print("JOB ID: %s\n" %(jobidchr))
#		os.remove(job_file)




	# 3. Join file

	print("\nMerging individual chromosomes into database")
	myargs = ["/home/proyectos/bioinfo/fjd/maf_fjd/newParser/dbPartitioning.sh", dbFile, "merging", dbUpdate]
	job_name =  "dbMerging"
	jobidchrStr = ':'.join(jobidlist)
	print("JOB NAME: %s\n" %(job_name))
	sbatch(job_name, args.output,' '.join(myargs), mail=args.mail,dep=jobidchrStr)


	

if __name__ == '__main__':
    main()





