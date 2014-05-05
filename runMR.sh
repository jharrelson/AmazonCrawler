#!/bin/bash
classname="AmazonCrawler"
sourcefile="$classname.java"
jarfile="$classname.jar"

#rm -Rf output*
rm -Rf classes
rm $jarfile
mkdir classes

export HADOOP_HOME="/home/$USER/software/hadoop-1.1.2"
export JAVA_HOME="/home/$USER/software/jdk1.7.0_25"

javac -cp $HADOOP_HOME/hadoop-core-1.1.2.jar:jsoup-1.7.3.jar:. -d classes *.java

jar -cvf $jarfile -C classes/ .
jar -uf $jarfile org/

rm -Rf *.tmp

