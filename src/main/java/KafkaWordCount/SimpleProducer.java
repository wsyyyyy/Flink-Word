package KafkaWordCount;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Properties;

public class SimpleProducer {
    public static void main(String[] args) throws Exception {
        // Assign topicName to string variable
        String topicName = "TextLinesTopic";
        // create instance for properties to access producer configs
        Properties props = new Properties();
        // Assign localhost id, 参考http://kafka.apache.org/documentation/#producerapi
        props.put("bootstrap.servers", "localhost:9092");
        props.put("zookeeper.connect", "localhost:2181");
        // Set acknowledgements for producer requests.
        props.put("acks", "all");
        // If the request fails, the producer can automatically retry,
        props.put("retries", 0);
        // Specify buffer size in config
        props.put("batch.size", 16384);
        // Reduce the no of requests less than 0
        props.put("linger.ms", 1);
        // The buffer.memory controls the total amount of memory available to the
        // producer for buffering.
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        // read a txt file , send it line by line
        File file = new File("/Users/sub/Desktop/120万单词.txt");
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String tempString = null;
        while ((tempString = reader.readLine()) != null) {
            producer.send(new ProducerRecord<String, String>(topicName, tempString));
            //Thread.sleep(100);
        }
        reader.close();
        System.out.println("Message sent successfully");
        producer.close();
    }
}
