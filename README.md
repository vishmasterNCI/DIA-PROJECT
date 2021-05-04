# DIA-PROJECT

The steps to install Docker is same as that of the tutorial
The Dockerfile will be uploaded on moodle,since it contains my password for github.

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
 
 unzip tweets.zip

 spark-submit producer.py > output.txt &
 
 python pandas-main.py 10000(or 20000)
 
 spark-submit spark-main.py 10000(2000)
 
 spark-submit map-reduce-main.py 10000(2000)
