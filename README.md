
A simple python MapReduce framework based on streaming.

# Install
* Download pymred.py to work direcory.
* Set environment (you can also send them through command options)
    
  * export HADOOPEXEC=/home/hadoop/current/bin/hadoop
  * export PYTHONEXEC=/usr/bin/python 
  * export STREAMINGJAR=/home/hadoop/current/contrib/streaming/hadoop-...-streaming.jar


# Quick Start


## Write wordcount.py

    import pymred
    def map(key, value):
        for word in value.split(" "):
            yield word, 1
    def reduce(key, values):
        yield key+" "+str(sum(values)), None
    pymred.run(map, reduce)

## Run wordcount.py

* run in hadoop

    python wordcount.py -input /path/of/hdfs/testin -output /path/of/hdfs/.../testout -numReduceTasks 10

* run locally (commonly used for testing the code)

    python wordcount.py -input ~/testin -output ~/testout -mode local
    
## Complete usage of wordcount.py 

    Usage wordcount.py
    Options:
      -input          <path>    DFS input file(s) for the Map step
      -output         <path>    DFS Output directory for the Reduce step
      -inputfmt       Text<Default>|Code|SequenceText|SequenceCode
      -outputfmt      Text<Default>|Code
      -mode           hadoop<default>|local	Folowing options will be ignored if local mode is set
      -hadoopexec     <hadoopexec>    Optional if environ variable "HADOOPEXEC" is set
      -pythonexec     <pythonexec>    Optional if environ variable "PYTHONEXEC" is set
      -streamingjar   <streamingjar>  Optional if environ variable "STREAMINGJAR" is set 
      -file           <file>    Optional  File/dir to be shipped in the Job jar file
      -partitioner    JavaClassName  Optional.
      -numReduceTasks <num>     Optional.
      -inputreader    <spec>    Optional.
      -cmdenv         <n>=<v>   Optional. Pass env.var to streaming commands
      -mapdebug       <path>    Optional. To run this script when a map task fails 
      -reducedebug    <path>    Optional. To run this script when a reduce task fails
      -verbose
    Generic options supported are
      -conf <configuration file>     specify an application configuration file
      -D <property=value>            use value for given property
      -fs <local|namenode:port>      specify a namenode
      -jt <local|jobtracker:port>    specify a job tracker
      -files <comma separated list of files>    specify comma separated files to be copied to the map reduce cluster
      -libjars <comma separated list of jars>   specify comma separated jar files to include in the classpath
      -archives <comma separated list of archives>    specify comma separated archives to be unarchived on the compute machines
    
    

