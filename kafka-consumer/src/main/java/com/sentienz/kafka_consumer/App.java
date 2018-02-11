package com.sentienz.kafka_consumer;
import java.util.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
public class App 
{
    public static void main( String[] args )
    {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092, localhost:9093, localhost:9094");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", "test");
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("auto.commit.interval.ms", "1000");
        properties.setProperty("auto.offset.reset", "earliest");
        
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList("MyTo"));
        
        while(true) {
        		ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(100);
        		for(ConsumerRecord<String, String> consumerRecord : consumerRecords) {
        			consumerRecord.value();
        			consumerRecord.key();
        			consumerRecord.offset();
        			consumerRecord.partition();
        			consumerRecord.topic();
        			consumerRecord.timestamp();
        			
        			System.out.println("partition: "+ consumerRecord.partition() + ", Offset: "+consumerRecord.offset() + ", key: "+ consumerRecord.key()+ ", Value: "+ consumerRecord.value());
        			
        		}
        }
        
    }
}
