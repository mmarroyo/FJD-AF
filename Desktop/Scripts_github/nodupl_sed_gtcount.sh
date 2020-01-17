#!/bin/bash
#SBATCH --job-name=noDuplGTcount   #job name
#SBATCH --mail-type=NONE # Mail events (NONE, BEGIN, END, FAIL, ALL)
##SBATCH --mail-user=marinaarroyomarta@gmail.com # Where to send mail
#SBATCH --mem-per-cpu=3gb # Per processor memory
#SBATCH --cpus-per-task=1
#SBATCH -t 15:00:00     # Walltime
#SBATCH -o /home/proyectos/bioinfo/fjd/maf_fjd/std/noDuplGTcount_%j.out # Name output file
#SBATCH -e /home/proyectos/bioinfo/fjd/maf_fjd/std/noDuplGTcount_%j.err
##SBATCH --file=
##SBATCH --initaldir=
## 1. Change column order (change_column_order.sh )

projectDir=$1
chr_file=$2
chr=$(basename $chr_file)
chr=${chr%_updated*}

sed '1d' $chr_file | awk -F"\t" '{print $1"\t"$2"\t"$3"\t"$4"\t"$6"\t"$7"\t"$8"\t"$9"\t"$11"\t"$12"\t"$14"\t"$15"\t"$17"\t"$18"\t"$20"\t"$21"\t"$23"\t"$24"\t"$26"\t"$27"\t"$29"\t"$30"\t"$32"\t"$33"\t"$36"\t"$37"\t"$38"\t"$39"\t"$41"\t"$42"\t"$44"\t"$45"\t"$47"\t"$48"\t"$50"\t"$51"\t"$53"\t"$54"\t"$56"\t"$57"\t"$59"\t"$60"\t"$62"\t"$63"\t"$65"\t"$66"\t"$68"\t"$69"\t"$71"\t"$72"\t"$74"\t"$75"\t"$77"\t"$78"\t"$80"\t"$81"\t"$83"\t"$84"\t"$86"\t"$87"\t"$89"\t"$90"\t"$92"\t"$93"\t"$95"\t"$96"\t"$98"\t"$99"\t"$100"\t"$101"\t"$102"\t"$103"\t"$104"\t"$105"\t"$106"\t"$107"\t"$108"\t"$109"\t"$110"\t"$111"\t"$112"\t"$113"\t"$114"\t"$115"\t"$116"\t"$117"\t"$118"\t"$119"\t"$120"\t"$121"\t"$122"\t"$123"\t"$124"\t"$125"\t"$126"\t"$127"\t"$128"\t"$5"\t"$10"\t"$13"\t"$16"\t"$19"\t"$22"\t"$25"\t"$28"\t"$31"\t"$34"\t"$35"\t"$40"\t"$43"\t"$46"\t"$49"\t"$52"\t"$55"\t"$58"\t"$61"\t"$64"\t"$67"\t"$70"\t"$73"\t"$76"\t"$79"\t"$82"\t"$85"\t"$88"\t"$91"\t"$94"\t"$97}' OFS="\t" |
## 2. Change 0.0 -- 0 and delete second row
sed ':a;s/\t\(\t\|$\)/\t0\1/;ta' | sed 's/\.0\b//g' | sed 's/\./\,/g' | 
## 2. Delete duplicates and get max value
/home/proyectos/bioinfo/fjd/maf_fjd/newParser/datamash-1.3/datamash -g1,2,3,4 max 5-97 > $projectDir"/"$chr"_NoDupl_GTcount"
