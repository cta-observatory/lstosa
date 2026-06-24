#!/bin/bash
while getopts d:t: flag
do
    case "${flag}" in
        d) date=${OPTARG};;
        t) datatype=${OPTARG};;
    esac
done
echo "Date: $date";
echo "Data type: $datatype";

#digits=`echo $date | grep -oE [[:digit:]] | wc -l`
digits=${#date}
echo $digits

if [ $digits == '8' ]; then
    runs=`ls /fefs/onsite/data/lst-pipe/LSTN-01/${datatype}/${date}/*Run* | awk -F '.' '{print $3}' | sort | uniq`
    echo $runs >> ${datatype}_${date}_count_subruns.txt
    
    if [ -f ${datatype}_${date}_count_subruns.txt ]; then
        echo "File ${datatype}_${date}_count_subruns.txt exists; deleting it"
        rm ${datatype}_${date}_count_subruns.txt
    fi
    
    for i in ${runs}; do
        run=(*${i}*)
        nsr=`ls /fefs/onsite/data/lst-pipe/LSTN-01/${datatype}/${date}/${run} | awk -F '.' '{print $4}' | sort | uniq | wc -l`
        echo $nsr | tr -d '\n' >> ${datatype}_${date}_count_subruns.txt
        echo "," | tr -d '\n' >> ${datatype}_${date}_count_subruns.txt
    done
fi

if [ $digits == '6' ]; then
    date_list=`ls -d /fefs/onsite/data/lst-pipe/LSTN-01/R0G/${date}* | awk -F '/' '{print $8}'`
    if [ -f ${datatype}_${date}_count_subruns.txt ]; then
        echo "File ${datatype}_${date}_count_subruns.txt exists; deleting it"
        rm ${datatype}_${date}_count_subruns.txt
    fi
    for day in $date_list; do 
       echo $day >> ${datatype}_${date}_count_subruns.txt
        runs=`ls /fefs/onsite/data/lst-pipe/LSTN-01/${datatype}/${day}/*Run* | awk -F '.' '{print $3}' | sort | uniq`
       echo $runs >> ${datatype}_${date}_count_subruns.txt

       for i in ${runs}; do
           run=(*${i}*)
           nsr=`ls /fefs/onsite/data/lst-pipe/LSTN-01/${datatype}/${day}/${run} | awk -F '.' '{print $4}' | sort | uniq | wc -l`
           echo ${i}":"$nsr | tr -d '\n' >> ${datatype}_${date}_count_subruns.txt
           echo "," | tr -d '\n' >> ${datatype}_${date}_count_subruns.txt
       done
       echo "" >> ${datatype}_${date}_count_subruns.txt
    done
fi
