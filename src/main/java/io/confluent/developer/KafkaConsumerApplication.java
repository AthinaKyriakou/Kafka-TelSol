package io.confluent.developer;

// write to topic producer_test
// read from consumer_test
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.apache.avro.generic.GenericRecord;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Random;


public class KafkaConsumerApplication {
	
	public static void main(String[] args) throws Exception{
		
		Properties props_producer = new Properties();
		Properties props_consumer = new Properties();
		props_producer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props_producer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
		props_producer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
		props_producer.put("schema.registry.url", "http://localhost:8081");
		
		props_consumer.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props_consumer.put(ConsumerConfig.GROUP_ID_CONFIG, "group0");
		props_consumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props_consumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
		props_consumer.put("schema.registry.url", "http://localhost:8081");
		props_consumer.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		
		KafkaProducer producer = new KafkaProducer(props_producer);
		final Consumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(props_consumer);
		
		

		String topic = "topic2";
		consumer.subscribe(Arrays.asList(topic));

		String message, key = "DEH";
		String userSchema = "{\"type\":\"record\"," +
							"\"name\":\"myrecord\"," +
							"\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}";
		int messages_produced = 0, synch_received = 0, asynch_received = 0;
		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(userSchema);

		//infinte poll loop
		while((messages_produced > asynch_received) || (messages_produced < 5)) {
			if(messages_produced < 5) {
				GenericRecord avroRecord = new GenericData.Record(schema);
				avroRecord.put("f1", "value" + Integer.toString(messages_produced));
				ProducerRecord<Object, Object> record = new ProducerRecord<>("topic1", key, avroRecord);
				
				producer.send(record);
				producer.flush();
				messages_produced++;

			}


			//read until you have received synch
			//read until you get the asynch
			if(messages_produced > synch_received) {
				while(messages_produced > synch_received) {
					ConsumerRecords<String, GenericRecord> records = consumer.poll(100); // 100 is how long the poll with block if no data
					for (ConsumerRecord<String, GenericRecord> record : records) {
						message = record.value();
						if((record.key()).toString().equals(key.toString()) && (messages_produced > synch_received)) {
							System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value() + "  synch???");
							synch_received++;
						}
						else {
							if((record.value()).contains(key.toString())) {
								System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value() + "  asynch special???");
								asynch_received++;
							}
						}
					}
				}
			}
			else {
				ConsumerRecords<String, GenericRecord> records = consumer.poll(100); // 100 is how long the poll with block if no data
				for (ConsumerRecord<String, GenericRecord> record : records) {
					if((record.value()).contains(key.toString())) {
						System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value() + "  asynch???");
						asynch_received++;
					}
					
				}
			}
			
		}


		//close producer if we ever want to terminate the while loop
		producer.close();
		consumer.close();

	}

}
