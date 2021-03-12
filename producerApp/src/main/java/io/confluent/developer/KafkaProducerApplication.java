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
import java.util.Scanner;



public class KafkaProducerApplication  {
	
	public static void main(String[] args) throws Exception{
		String key = "WRONG";
		int synch_received = 0, asynch_received = 0;
		
		


		Properties props_producer = new Properties();
		Properties props_consumer = new Properties();
		props_producer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props_producer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
		props_producer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
		props_producer.put("schema.registry.url", "http://localhost:8081");
		
		props_consumer.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props_consumer.put(ConsumerConfig.GROUP_ID_CONFIG, "group_paroxos");
		props_consumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props_consumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
		props_consumer.put("schema.registry.url", "http://localhost:8081");
		props_consumer.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		
		KafkaProducer producer = new KafkaProducer(props_producer);
		final Consumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(props_consumer);
		final Consumer<String, String> login_consumer = new KafkaConsumer<String, String>(props_consumer);
		
		String topic = "ack";
		consumer.subscribe(Arrays.asList(topic));
		login_consumer.subscribe(Arrays.asList("validated_user_data"));
		

		String userSchema = "{\"type\":\"record\"," +
							"\"name\":\"myrecord\"," +
							"\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}";
		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(userSchema);

		String loginSchema = "{\"type\":\"record\"," +
							"\"name\":\"user_data\"," +
							"\"fields\":[{\"name\":\"Username\",\"type\":\"string\"},{\"name\":\"Password\",\"type\":\"string\"}]}";
		Schema.Parser login_parser = new Schema.Parser();
		Schema login_schema = login_parser.parse(loginSchema);

		//login
		String login = "failed";
		Scanner myObj = new Scanner(System.in);

		while(login != "success"){
			
			System.out.printf("\n\n Please enter Details:\n Username: ");
			String username = myObj.nextLine();
			System.out.printf("\n Password: ");
			String password = myObj.nextLine();
			
			GenericRecord login_avroRecord = new GenericData.Record(login_schema);
			login_avroRecord.put("Username", username);
			login_avroRecord.put("Password", password);
			ProducerRecord<Object, Object> login_record = new ProducerRecord<>("user_data", key, login_avroRecord);
			producer.send(login_record);
			producer.flush();
			int temp = 0;
			while(temp == 0){
				temp++;
				//ConsumerRecords<String, String> records = login_consumer.poll(100);
				//for(ConsumerRecord<String, String> validation_record : records) {
				//	if((validation_record.key()).toString().equals(key.toString())){
				//		login = validation_record.value();
				//	}
				//}
			}
			login = "success";
			if(login == "failed"){
				System.out.printf("Login Failed, Please Check you Username and Password and Retry \n\n\n");
			}
		}

		// read json file
		// convert json to avro
		//send avro and receive synch
		
		GenericRecord avroRecord = new GenericData.Record(schema);
		avroRecord.put("f1", "myote");
		ProducerRecord<Object, Object> new_record = new ProducerRecord<>("insertion", key, avroRecord);
		System.out.printf("producing recordddddddddddddddddddddddddddddddddddddddddd \n\n\n");
		producer.send(new_record);
		producer.flush();


		//read until you have received synch
		while(synch_received == 0) {
			ConsumerRecords<String, GenericRecord> records = consumer.poll(100); // 100 is how long the poll with block if no data
			for (ConsumerRecord<String, GenericRecord> record : records) {
				if((record.key()).toString().equals(key.toString())) {
					System.out.printf("\noffset = %d, key = %s, value = %s\n\n", record.offset(), record.key(), record.value());
					synch_received++;
				}
			}
		}

		int count_time = 0;
		String myresult = "";
		while(asynch_received == 0){
			if(count_time == 5){
				System.out.printf(" Waiting for Asynchronous reply, While doing nothing\n\n\n");
				count_time = 0;
			}
			try
			{
				Thread.sleep(40);
				count_time++;
			}
			catch(InterruptedException ex)
			{
				Thread.currentThread().interrupt();
			}

			ConsumerRecords<String, GenericRecord> records = consumer.poll(100); // 100 is how long the poll with block if no data
			for (ConsumerRecord<String, GenericRecord> record : records) {
				if((record.key()).toString().equals(key.toString())) {
					//System.out.printf("offset = %d, key = %s, value = %s\n\n", record.offset(), record.key(), record.value());
					asynch_received++;
					myresult = (record.value()).get("ACK_Message").toString();
				}
			}
		}
		String mystring = (myresult.split("Status: "))[1];
		System.out.printf(mystring + "\n\n\n\n\n\n", myresult);


		//close producer if we ever want to terminate the while loop
		producer.close();
		consumer.close();

	}

}
