# Flink-examples (for Flink 1.6.2)

Collection of common Flink usage and utilities.
At the moment, there are only the following jobs:

* [Csv2RowExample](https://github.com/okkam-it/flink-examples/blob/master/src/main/java/it/okkam/datalinks/batch/flink/datasourcemanager/importers/Csv2RowExample.java): shows how to generate a Flink __DataSet<Row>__ from a CSV file, using
    * __flink-table-api__ : doesn't hangle properly string fields containing double quoted tokens (see https://issues.apache.org/jira/browse/FLINK-4785)
    * __apache commons-csv__ : reads all fields as string
* [ElastisearchHelper](https://github.com/okkam-it/flink-examples/blob/master/src/main/java/it/okkam/datalinks/batch/flink/elasticsearch/ElasticsearchHelper.java): shows how to create elasticsearch index templates and index mappings, allowing
	* number of shards configuration: needed in most cases (see https://issues.apache.org/jira/browse/FLINK-4491)
	* number of replicas configuration
	* stop words, filter and mappings configuration
* [KafkaFlinkAvroParquet](https://github.com/okkam-it/flink-examples/blob/master/src/main/java/org/okkam/flink/KafkaFlinkAvroParquet.java): shows how to integrate kafka, flink, avro and parquet. In particular
	* AvroDeserializationSchema: deserialize a <T> object to byte[]
	* AvroSerializationSchema: serialize the deserialized byte[] to a <T> object
	* deserialized object are passed through a Kafka queue
* [JsonStringToPojo](https://github.com/okkam-it/flink-examples/blob/master/src/main/java/it/okkam/flink/json/JsonStringToPojo.java): Read json file and deserialize to Pojo
* [Pojo2JsonString](https://github.com/okkam-it/flink-examples/blob/master/src/main/java/it/okkam/flink/json/Pojo2JsonString.java): Serialize Pojo to json and write to file

### Working with Kafka (version 2.11-0.10.2.0)

To set up the Kafka testing environment [download](https://www.apache.org/dyn/closer.cgi?path=/kafka/0.10.2.0/kafka_2.11-0.10.2.0.tgz) the release and un-tar it:

```
> tar -xzf kafka_2.11-0.10.2.0.tgz
> cd kafka_2.11-0.10.2.0
```

##### Start ZooKeeper server

Kafka runs over ZooKeeper so first start the ZooKeeper server that is packaged with Kafka to run a single-node ZooKeeper instance. The .properties file is already configured in order to start the ZooKeeper server on port 2181:

```
> bin/zookeeper-server-start.sh config/zookeeper.properties
```

##### Start Kafka server

Now is possible to run the Kafka server (broker) that will start on port 9092:

```
> bin/kafka-server-start.sh config/server.properties
```

##### Create a topic

In order to start communicating a new topic have to be created: 

```
> bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
```

The example works only with a topic named "test", ZooKeeper on port 2181 and Kafka on port 9092. If you want to change the the topic name or the ports, remember to modify also the java code:

```java
static String topicId = "test";
static String kafkaPort = "localhost:9092";
static String zkPort = "localhost:2181";
  ```
##### Test it

The Producer and the Consumer are automatically managed by the example class that generates, sends and retrieves messages through the kafka queue. Just run the [KafkaFlinkAvroParquet](https://github.com/okkam-it/flink-examples/blob/master/src/main/java/org/okkam/flink/KafkaFlinkAvroParquet.java) class. 
