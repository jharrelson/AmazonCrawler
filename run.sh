#!/bin/bash
printf " \$\$ Compiling code...\n"
./runMR.sh

printf "\n \$\$ Copying movie dataset from newscratch...\n"
cp /newscratch/lngo/dataset/project/amazon/movie/movies.txt.gz ./

printf "\n \$\$ Copying food dataset from newscratch...\n"
cp /newscratch/lngo/dataset/project/amazon/food/finefoods.txt.gz ./

printf "\n \$\$ Extracting movie dataset...\n"
gunzip movies.txt.gz

printf "\n \$\$ Extracting food dataset...\n"
gunzip finefoods.txt.gz

printf "\n \$\$ Merging datasets into large.dat...\n"
cat finefoods.txt > large.dat
cat movies.txt >> large.dat

printf "\n \$\$ Copying large.dat to hdfs...\n"
hadoop fs -copyFromLocal large.dat /large.dat

printf "\n \$\$ Executing mapreduce...\n"
hadoop jar Amazon.jar Amazon /large.dat $USER $1