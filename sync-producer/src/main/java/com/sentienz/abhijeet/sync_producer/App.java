package com.sentienz.abhijeet.sync_producer;
import java.util.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
public class App{
    public static void main( String[] args ){
    		Scanner s = new Scanner(System.in);
    		while(true) {
    			String topic = "MyTo";
    			String key = "key3";
    			System.out.println("Enter your message:");
    			String value = s.nextLine();
    			Properties properties = new Properties();
    			properties.setProperty("bootstrap.servers", "localhost:9092, localhost:9093, localhost:9094");
    			properties.setProperty("key.serializer", StringSerializer.class.getName());
    			properties.setProperty("value.serializer", StringSerializer.class.getName());
    			Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(properties);
    			ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, key, value);
    			try {
    				RecordMetadata metadata = producer.send(producerRecord).get();
    				System.out.println("Message is sent to Partition no: " + metadata.partition());
    				System.out.println("SynchronousProducer Completed with success");
    			}catch(Exception e){
    				e.printStackTrace();
    				System.out.println("SynchronousProducer failed with an exception");
    			}finally {
    				producer.close();
    			}
    		}
    }
}
