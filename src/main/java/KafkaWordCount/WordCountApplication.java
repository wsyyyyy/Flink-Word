package KafkaWordCount;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

/*
 to run this demo, need to:
 1. start zookeeper and kafka
 2. create topic TextLinesTopic and WordsWithCountsTopic
 * bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic TextLinesTopic
 * bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic WordsWithCountsTopic
 3.make data source file
 4. run producer and consumer

 */
public class WordCountApplication {
    public static void main(final String[] args) throws Exception {
        Properties props = new Properties();
        // Kafka Streams requires at least the following properties "application.id"，"bootstrap.servers"
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
        // config kafka location
        // {@see CommonClientConfigs#BOOTSTRAP_SERVERS_DOC}, config "ubuntu-02 192.168.78.132" in the hosts file in advance
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // provide defualt serdes(serializer and deserializer)
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // config the processor with DSL(Domain Specified Language)
        StreamsBuilder builder = new StreamsBuilder();
        // create a source stream from a Kafka topic named TextLinesTopic
        KStream<String, String> textLines = builder.stream("TextLinesTopic");
        KTable<String, Long> wordCounts = textLines
                // Create new KStream: by implements {@link ValueMapper} transform map textLine to String array
                .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
                // select value as key
                .selectKey((key, word) -> word)
                .groupByKey()
                // count the number of records in this new stream. use default store
                .count(Materialized.as("counts-store"));

        // write this new kstream into another Kafka topic named WordsWithCountsTopic
        wordCounts.toStream().to("WordsWithCountsTopic", Produced.with(Serdes.String(), Serdes.Long()));

        // inspect what kinds of topology is created
        Topology topology = builder.build();
        System.out.println(topology.describe());

        // create kafka stream
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
    }

}
