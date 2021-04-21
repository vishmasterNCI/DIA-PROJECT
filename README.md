# DIA-PROJECT

Steps for running after starting the image
 DIA project
 
 conda init bash
 
 source ~/.bashrc
 
 conda activate dia
 
 pip install -r requirements.txt

./hadoop/sbin/start-dfs.sh

./hadoop/sbin/start-yarn.sh

 zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties &
 
 kafka-server-start.sh $KAFKA_HOME/config/server.properties &
 
 python producer.py > output.txt &
 
 python pandas-main.py 500(or 1000 ,2000)
 
 spark-submit spark-main.py 500(or 1000 ,2000)
 
 spark-submit map-reduce-main.py 500(or 1000 ,2000)
