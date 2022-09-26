#Introduction

This project involves building a datapipeline using twitter streaming and sparkstreaming for cs523 course final project.

#Getting Started.
There are two sub projects, first project is The KafkaProducer project which makes use of twitter filtered stream to pull filtered stream of tweets and persist in kafka messaging queue, and the second part is kafkaConsumer which makes use of sparkStreaming to consume the tweets and save the processed tweets in hbase.

## Dependency

### Java 1.8+

### Kafka
1. Install kafka by following instructions from this link https://www.javatpoint.com/installing-kafka-on-linux, and move to `/usr/lib` then run `export KAFKA_HOME=/usr/lib/<kafka_directory_name>/`

2. start zookeper
`sudo ./zkServer.sh start`

3. start kafka
`$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties`

### HBase
1. Start hbase master
`sudo service hbase-master start;`
2. Start hbase region server
`sudo service hbase-regionserver start;`

### Running the project

1. KafkaProducer: \
Run `TwitterProducer.java` in project from eclipse and provide 2 program arguments \
example: en #covid19. or run jar via spark-submit
2. KafkaConsumer: \
run `SparkStreaming.java` in project from eclipse or run jar via spark-submit