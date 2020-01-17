
### Script for running each chr in a batch.
import argparse
import glob
import os
import sys
import subprocess
import shutil

#print('hola')
def sbatch(job_name, folder_out, command, mem=6, time=4000000, threads=1, mail=None, dep='', wait = ''):

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

        mail="marinaarroyomarta@gmail.com"
        STDoutput = "/home/proyectos/bioinfo/fjd/maf_fjd/std"
        NdbFolder = "/home/proyectos/bioinfo/fjd/maf_fjd/dbs"
        GTdbFolder="/home/proyectos/bioinfo/fjd/maf_fjd/dbs/GT"

        # LIST OF CHRS:
        list_of_chrs = glob.glob(NdbFolder+'/chr*')
        list_of_chrs = [i.split("/")[-1] for i in list_of_chrs]

        last_db="TSO43_TSO33_TSO28_TSO42_TSO39_TSO36_TSO32_TSO54_TSO38_TSO47_TSO37_TSO45_TSO35_TSO46_TSO41_TSO44_TSO25_TSO52_TSO40_TSO53_TSO50_TSO30_TSO49_TSO34_TSO51_TSO29_TSO27_TSO48_TSO31Sophia_e_chr"


        for chr in list_of_chrs:

            Nfile = NdbFolder+"/"+chr+"/"+last_db+'*'
            GTfile = GTdbFolder+"/"+chr+"/"+last_db+'*'

            outputMergeFiles = "/home/proyectos/bioinfo/fjd/maf_fjd/dbs/mergeFiles/"+chr+"/"+"db_"+chr+".txt"

            myargs = ["/home/proyectos/bioinfo/fjd/maf_fjd/newParser/merge_files_N_GTcount.sh", GTfile, Nfile, outputMergeFiles]

            #merged_files_list.append(outputMergeProjects)

            job_name =  "dbMerging"
            print("JOB NAME: %s\n" %(job_name))
            job_id = sbatch(job_name, STDoutput,' '.join(myargs), mail=mail)


if __name__ == '__main__':
    main()
