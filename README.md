# DIA-PROJECT
The main idea behind this project was to compare and analyse the performance of traditional data science tools on Big data. 
This project aims to do online sentiment analysis on a stream of data from twitter.Since I wanted to try my hands on a wide range of tools,I have used Kafka to generate the stream instead of the traditional twitter+python/twitter+spark stream.Im not actually continuously generating the stream,instead downloading the data and then producing a continuos stream.But the improvement that could be made is to continuously generate it.

This project assumes you have a pseudo distributed HDFS system(single node),Hadoop ,Spark and Kafka pre-installed.

#The Dockerfile will be provided on request only

Steps for running after starting the container

 
 conda init bash
 
 source ~/.bashrc
 
 conda activate dia
 
 cd DIA-PROJECT
 
 pip install -r requirements.txt

./hadoop/sbin/start-dfs.sh

./hadoop/sbin/start-yarn.sh

 zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties &
 
 kafka-server-start.sh $KAFKA_HOME/config/server.properties &
 

 hdfs dfs -mkdir -p /user/Hadoop/twitter_data
 
 python producer.py > output.txt &

 unzip tweets.zip

 spark-submit producer.py > output.txt &

 
 python pandas-main.py 10000(or 20000)
 
 spark-submit spark-main.py 10000(2000)
 
 spark-submit map-reduce-main.py 10000(2000)
