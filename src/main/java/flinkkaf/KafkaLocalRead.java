package flinkkaf;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;

public class KafkaLocalRead {
    private static final String broker_list = "localhost:9092";
    private static final String topic = "test2";  //kafka topic 需要和 flink 程序用同一个 topic
    static Producer<String, String> producer = null;
    public static void writeToKafka() throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("acks", "1");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(props);

        String runtime = new Date().toString();

        File file = new File("/Users/sub/Downloads/英语四级历年真题/2018年6月英语四级真题及答案【博宇教育QQ微信：582622214】.docx");
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            //一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                producer.send(
                        new ProducerRecord<String, String>("test2", Integer.toString(line), tempString));
                System.out.println("line " + line + ": " + tempString);
                Thread.sleep(100);
                line++;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
        producer.flush();
    }
    public static void main(String[] args) throws InterruptedException {
        writeToKafka();
    }
}

