#!/usr/bin/env bash

CURRENT=$(dirname "$0")
cd ${CURRENT}
HOMEDIR=`pwd`/..
TMPDIR=/home/spark/sparkgatk/run/tmp
LIBSDIR=${HOMEDIR}/lib
SPARK_HOME=/home/spark/Softwares/spark

THREAD=6

db=/home/spark/GATK/known_database
mills_1kg=${db}/1000G_gold_standard/Mills_and_1000G_gold_standard.indels.b37.vcf.gz
cosmic=${db}/COSMIC/b37_cosmic_v73_061615.vcf.gz
targetBed=/home/spark/GATK/target.bed
dbsnp_del100=${db}/dbSNP/dbsnp_138.b37.vcf.gz
lib_path=/home/spark/sparkgatk/bin/JNILib
localname=gpu-server5

${SPARK_HOME}/bin/spark-submit \
        --class NGSSpark \
        --master spark://${localname}:7077 \
        --conf spark.executor.cores=1 \
        --conf spark.cores.max=4 \
        --conf spark.driver.memory=1g \
        --conf spark.executor.memory=20g \
        --conf spark.executor.heartbeatInterval=10000000 \
        --conf spark.network.timeout=10000000 \
        --jars ${LIBSDIR}/optparse_2.11-2.2.jar,${LIBSDIR}/htsjdk-1.4.1.jar,${LIBSDIR}/hadoop-bam-7.8.1-SNAPSHOT-jar-with-dependencies.jar \
        ${HOMEDIR}/target/scala-2.11/sparkgatk_2.11-1.0.jar \
        -B /home/spark/sparkgatk/bin \
        -n 20 \
        -t ${THREAD} \
        -I /user/spark/data_L001/case_64M \
        -I /user/spark/data_L001/normal_64M \
        --index /home/spark/GATK/reference_sequence/hs37d5.fasta \
        --read_group "ID:normal LB:normalLib SM:normal PU:runname PL:illumina" \
        --read_group "ID:case LB:caseLib SM:case PU:runname PL:illumina" \
        --local_tmp ${TMPDIR} \
        --hdfs_tmp /user/spark/sparkgatk_tmp \
        --CA java=="-d64 -server" \
        --CA java_markduplicates=="-Xms20g -Xmx20g" \
        --CA java_realignertargetcreator=="-Xms20g -Xmx20g -Djava.library.path=$lib_path" \
        --CA gatk_realignertargetcreator=="-known $mills_1kg -known $dbsnp_del100 -L $targetBed -nt $THREAD -allowPotentiallyMisencodedQuals -rf NotPrimaryAlignment -dt NONE" \
        --CA java_indelrealigner=="-Xms20g -Xmx20g -Djava.library.path=$lib_path" \
        --CA gatk_indelrealigner=="-known $mills_1kg -known $dbsnp_del100 -allowPotentiallyMisencodedQuals -rf NotPrimaryAlignment -dt NONE --maxReadsForRealignment 10000000" \
        --CA java_baserecalibrator=="-Xms20g -Xmx20g -Djava.library.path=$lib_path" \
        --CA gatk_baserecalibrator=="-knownSites $mills_1kg -knownSites $dbsnp_del100 -knownSites $cosmic -nct $THREAD -allowPotentiallyMisencodedQuals -L $targetBed -rbs 3000" \
        --CA java_printreads=="-Xms20g -Xmx20g -Djava.library.path=$lib_path" \
        --CA gatk_printreads=="-nct $THREAD -allowPotentiallyMisencodedQuals -rbs 3000" \
        --CA java_variantcaller=="-Xms20g -Xmx20g -Djava.library.path=$lib_path" \
        --CA gatk_variantcaller=="-L $targetBed --dbsnp $dbsnp_del100" \
        --CA java_mutect2=="-Xms20g -Xmx20g -Djava.library.path=$lib_path" \
        --CA gatk_mutect2=="-L $targetBed --dbsnp $dbsnp_del100 --cosmic $cosmic -contamination 0 --max_alt_alleles_in_normal_count 3 --max_alt_alleles_in_normal_qscore_sum 40 --max_alt_allele_in_normal_fraction 0.02 -dt NONE" \
