
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

        #arguments
        # parser = argparse.ArgumentParser(description="UPDATE MAF DATABASE")
        # #parser.add_argument('-p', '--projectFolder', help='\t\tproject folder', required=True)
        # parser.add_argument('-v', '--vcfFolder', help='\t\tvcf folder', required=True)
        # parser.add_argument('-g', '--gvcfFolder', help='\t\tg.vcf folder', required=True)
        # parser.add_argument('-o', '--output', help='\t\toutput folder', required=True)
        # parser.add_argument('-n', '--name', help='\t\tproject name', required=True)
        #
        # #parser.add_argument('-t', '--threads' , help='\t\tNumber of threads', required=False, default=1)
        #
        # args = parser.parse_args()
        mail="marinaarroyomarta@gmail.com"
        STDoutput = "/home/proyectos/bioinfo/fjd/maf_fjd/std"
        dbFolder = "/scratch/ionut/ionut_TSO/TSO/"

        # LIST OF CHRS:
        list_of_chrs = glob.glob(dbFolder+'TSO28_12-06-2017/chr*.db.txt_updated_AF')
        list_of_chrs = [i.split("/")[-1].split(".")[0] for i in list_of_chrs]


        list_project = glob.glob(dbFolder+'TSO*')
        n_project = len(list_project)

        print('Lista projects:',list_project)
        print(n_project)

        merged_files_list=[]

        for chr in list_of_chrs:
            job_id_list = []
            merged_files_list=[]

            for x in range(1,n_project,2):

                project1_chr_file = list_project[x-1]+"/"+chr+".db.txt_NoDupl_GTcount"
                project2_chr_file = list_project[x]+"/"+chr+".db.txt_NoDupl_GTcount"
                project1_name = list_project[x-1].split("/")[-1].split("_")[0]
                project2_name = list_project[x].split("/")[-1].split("_")[0]

                outputMergeProjects = "/home/proyectos/bioinfo/fjd/maf_fjd/dbs/GT/"+chr+"/"+project1_name+"_"+project2_name+"_e_"+chr+".txt" #con ruta y directorio especfico de chrom"

                print("\nMerging individual chromosomes into database")
                myargs = ["/home/proyectos/bioinfo/fjd/maf_fjd/newParser/merge_projectsChr_GTcount.sh", project1_chr_file, project2_chr_file, outputMergeProjects]

                merged_files_list.append(outputMergeProjects)

                job_name =  "dbMerging"
                print("JOB NAME: %s\n" %(job_name))
                job_id = sbatch(job_name, STDoutput,' '.join(myargs), mail=mail)
                job_id_list.append(job_id)
                # add dependency


            if n_project%2.0 != 0:
                project1_name = list_project[-1].split("/")[-1].split("_")[0]
                outputMergeProjects = "/home/proyectos/bioinfo/fjd/maf_fjd/dbs/GT/"+chr+"/"+project1_name+"_e_"+chr+".txt"
                shutil.copy(list_project[-1]+"/"+chr+".db.txt_NoDupl_GTcount", outputMergeProjects)
                #os.copy(list_project[-1], outputMergeProjects)
                merged_files_list.append(outputMergeProjects)


            nfiles = len(merged_files_list)

            while nfiles>1:

                run_merged_files_list = merged_files_list

                merged_files_list = []

                run_job_ids = job_id_list


                for x in range(1,nfiles,2):

                    project1_chr_file = run_merged_files_list[x-1]
                    project2_chr_file = run_merged_files_list[x]
                    project1_name = run_merged_files_list[x-1].split("/")[-1].split("_e_")[0]
                    project2_name = run_merged_files_list[x].split("/")[-1].split("_e_")[0]

                    outputMergeProjects = "/home/proyectos/bioinfo/fjd/maf_fjd/dbs/GT/"+chr+"/"+project1_name+"_"+project2_name+"_e_"+chr+".txt"

                    myargs = ["/home/proyectos/bioinfo/fjd/maf_fjd/newParser/merge_projectsChr_GTcount.sh", project1_chr_file, project2_chr_file, outputMergeProjects]

                    merged_files_list.append(outputMergeProjects)

                    job_name =  "dbMerging2"
                    print("JOB NAME: %s\n" %(job_name))
                    job_id_list_Str = ':'.join(run_job_ids)
                    job_id = sbatch(job_name, STDoutput,' '.join(myargs), mail=mail, dep=job_id_list_Str)
                    job_id_list.append(job_id)

                if nfiles%2.0 != 0:
                    merged_files_list.append(run_merged_files_list[-1])
                nfiles = len(merged_files_list)

if __name__ == '__main__':
    main()
