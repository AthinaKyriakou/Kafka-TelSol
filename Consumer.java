package mykafka;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;


public class by {
	
	public static void main(String[] args) {
		
		//consumer Properties
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");//bootstrap list of kafka brokers
		props.put("group.id", "test-group");
		
		//using auto commit--> used for the offset to be commited automaticaly
		props.put("enable.auto.commit", "true");
		
		//string inputs and outputs--> deserializer is the opposite to serializer
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		
		//kafka consumer object
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		
		//subscribe topic
		consumer.subscribe(Arrays.asList("source-topic"));
		
		//infinte poll loop
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(100); // 100 is how long the poll with block if no data
			for (ConsumerRecord<String, String> record : records)
				System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
		}
	}

}
