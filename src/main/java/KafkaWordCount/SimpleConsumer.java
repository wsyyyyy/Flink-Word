package KafkaWordCount;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SimpleConsumer {
    // 判断一个字符串是否含有数字
    private static boolean HasDigit(String content) {
        boolean flag = false;
        Pattern p = Pattern.compile(".*\\d+.*");
        Matcher m = p.matcher(content);
        if (m.matches()) {
            flag = true;
        }
        return flag;
    }
    public static void main(String[] args) throws Exception {
        // Kafka consumer configuration settings
        String topicName = "WordsWithCountsTopic";
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
        KafkaConsumer<String, Integer> kafkaConsumer = new KafkaConsumer<>(props);
        // Kafka Consumer subscribes list of topics here.
        kafkaConsumer.subscribe(Arrays.asList(topicName));

        while(true) {
            ConsumerRecords<String, Integer> records = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, Integer> record : records) {
                // print the offset,key and value for the consumer records.
                if(record.key().length() > 1
                        && !HasDigit(record.key()) && !record.key().equals("the") && !record.key().equals("and")
                        && !record.key().equals("or") && !record.key().equals("but") && !record.key().equals("so")
                        && !record.key().equals("you")  && !record.key().equals("he") && !record.key().equals("it")
                        && !record.key().equals("she") && !record.key().equals("they") && !record.key().equals("an")
                        && !record.key().equals("is") && !record.key().equals("am") && !record.key().equals("are")
                        && !record.key().equals("of") && !record.key().equals("to") && !record.key().equals("in")
                        && !record.key().equals("that") && !record.key().equals("this") && !record.key().equals("be")
                        && !record.key().equals("one") && !record.key().equals("two") && !record.key().equals("three")
                        && !record.key().equals("part") &&!record.key().equals("questions") && !record.key().equals("passage")
                        && !record.key().equals("what") && !record.key().equals("which") && !record.key().equals("who")
                        && !record.key().equals("where") && !record.key().equals("when") && !record.key().equals("why")
                        && !record.key().equals("on") && !record.key().equals("at") && !record.key().equals("was")
                        && !record.key().equals("from") && !record.key().equals("with") && !record.key().equals("as")
                        && !record.key().equals("about") && !record.key().equals("by") && !record.key().equals("than")
                        && !record.key().equals("not") && !record.key().equals("how") && !record.key().equals("had")
                        && !record.key().equals("has") && !record.key().equals("been") && !record.key().equals("can")
                        && !record.key().equals("etc") && !record.key().equals("its") && !record.key().equals("do")
                        && !record.key().equals("we") && !record.key().equals("their") && !record.key().equals("many")
                        && !record.key().equals("up") && !record.key().equals("too") && !record.key().equals("them")
                        && !record.key().equals("more") && !record.key().equals("there") && !record.key().equals("your")
                        && !record.key().equals("our") && !record.key().equals("out") && !record.key().equals("would")
                        && !record.key().equals("will") && !record.key().equals("does") && !record.key().equals("then")
                        && !record.key().equals("me") && !record.key().equals("most") && !record.key().equals("may")
                        && !record.key().equals("his") && !record.key().equals("her") && !record.key().equals("each")
                        && !record.key().equals("just") && !record.key().equals("also") && !record.key().equals("were")
                        && !record.key().equals("should") && !record.key().equals("if") && !record.key().equals("no")
                        && !record.key().equals("some") && !record.key().equals("now") && !record.key().equals("well")
                        && !record.key().contains("_") && !record.key().equals("for") && !record.key().equals("answer")
                        && !record.key().equals("my") && !record.key().equals("re") && !record.key().equals("section")
                        && !record.key().matches("[\u4E00-\u9FA5]+")) {
                    System.out.printf("key = %s, value = %s\n",record.key(), record.value());
                }
            }
        }
    }
}
