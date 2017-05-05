package org.okkam.flink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.okkam.flink.avro.AvroDeserializationSchema;
import org.okkam.flink.avro.AvroSerializationSchema;
import org.okkam.flink.parquet.avro.Person;
import org.okkam.flink.parquet.avro.PhoneNumber;
import org.okkam.flink.parquet.avro.PhoneType;

import java.util.Arrays;
import java.util.Properties;

public class KafkaFlinkAvroParquet {

  static String topicId = "test";
  static String kafkaPort = "localhost:9092";
  static String zkPort = "localhost:2181";

  static SerializationSchema<Person> ser = new AvroSerializationSchema<Person>(Person.class);
  static DeserializationSchema<Person> deser = new AvroDeserializationSchema<Person>(Person.class);

  /**
   * Kafka test.
   * 
   * @param args
   *          the program arguments
   * 
   * @throws Exception
   *           if any exception occurs
   */
  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    Person person = new Person();
    person.setId(42);
    person.setName("Felix");
    person.setEmail("test@test.com");
    person.setPhone(Arrays.asList(new PhoneNumber("123456", PhoneType.WORK)));

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", kafkaPort);
    properties.setProperty("group.id", topicId);
    properties.setProperty("zookeeper.connect", zkPort);
    properties.setProperty("batch.size", "0");

    TypeInformation<Person> typeInfo = TypeExtractor.getForClass(Person.class);
    DataStream<Person> stream = env.fromCollection(Arrays.asList(person), typeInfo);
    stream.addSink(new FlinkKafkaProducer010<Person>(topicId, ser, properties));

    DataStreamSource<Person> messageStream = env
        .addSource(new FlinkKafkaConsumer010<Person>(topicId, deser, properties));
    messageStream.print();

    env.execute();

  }

}
