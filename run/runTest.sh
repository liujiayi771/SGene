#!/usr/bin/env bash

CURRENT=$(dirname "$0")
cd ${CURRENT}
HOMEDIR=`pwd`/..
TMPDIR=/home/spark/NGS-Spark/run/tmp
LIBSDIR=${HOMEDIR}/lib

THREAD=6

db=/home/spark/GATK/known_database
mills_1kg=${db}/1000G_gold_standard/Mills_and_1000G_gold_standard.indels.b37.vcf.gz
cosmic=${db}/COSMIC/b37_cosmic_v73_061615.vcf.gz
targetBed=/home/spark/GATK/target.bed
dbsnp_del100=${db}/dbSNP/dbsnp_138.b37.vcf.gz
lib_path=/home/spark/NGS-Spark/bin/JNILib
localname=gpu-server5

${SPARK_HOME}/bin/spark-submit \
        --class edu.hust.elwg.NGSSpark \
        --master spark://${localname}:7077 \
        --conf spark.executor.cores=1 \
        --conf spark.cores.max=8 \
        --conf spark.driver.memory=1g \
        --conf spark.executor.memory=30g \
        --conf spark.executor.heartbeatInterval=10000000 \
        --conf spark.network.timeout=10000000 \
        ${HOMEDIR}/target/scala-2.11/ngs-spark-assembly-0.1-SNAPSHOT.jar \
        -B /home/spark/NGS-Spark/bin \
        --bed ${targetBed} \
        -n 200 \
        -t ${THREAD} \
        -I /user/spark/data/case_40M \
        -I /user/spark/data/normal_40M \
        --index /home/spark/GATK/reference_sequence/hs37d5.fasta \
        --read_group "ID:normal LB:normalLib SM:normal PU:runname PL:illumina" \
        --read_group "ID:case LB:caseLib SM:case PU:runname PL:illumina" \
        --local_tmp ${TMPDIR} \
        --hdfs_tmp /user/spark/sparkgatk_tmp \
        --CA java=="-d64 -server" \
        --CA java_markduplicates=="-Xms20g -Xmx20g" \
        --CA java_realignertargetcreator=="-Xms20g -Xmx20g -Djava.library.path=$lib_path" \
        --CA gatk_realignertargetcreator=="-L $targetBed -known $mills_1kg -known $dbsnp_del100 -nt $THREAD -allowPotentiallyMisencodedQuals -rf NotPrimaryAlignment -dt NONE" \
        --CA java_indelrealigner=="-Xms20g -Xmx20g -Djava.library.path=$lib_path" \
        --CA gatk_indelrealigner=="-known $mills_1kg -known $dbsnp_del100 -allowPotentiallyMisencodedQuals -rf NotPrimaryAlignment -nThreads $THREAD -dt NONE --maxReadsForRealignment 10000000" \
        --CA java_baserecalibrator=="-Xms20g -Xmx20g -Djava.library.path=$lib_path" \
        --CA gatk_baserecalibrator=="-L $targetBed -knownSites $mills_1kg -knownSites $dbsnp_del100 -knownSites $cosmic -nct $THREAD -allowPotentiallyMisencodedQuals -rbs 3000" \
        --CA java_printreads=="-Xms20g -Xmx20g -Djava.library.path=$lib_path" \
        --CA gatk_printreads=="-nct $THREAD -allowPotentiallyMisencodedQuals -rbs 3000" \
        --CA java_mutect2=="-Xms20g -Xmx20g -Djava.library.path=$lib_path" \
        --CA gatk_mutect2=="--dbsnp $dbsnp_del100 --cosmic $cosmic -contamination 0 --max_alt_alleles_in_normal_count 3 --max_alt_alleles_in_normal_qscore_sum 40 --max_alt_allele_in_normal_fraction 0.02 -dt NONE -ntLib $THREAD"