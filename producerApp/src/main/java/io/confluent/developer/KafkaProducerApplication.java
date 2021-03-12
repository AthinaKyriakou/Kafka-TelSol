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

import java.nio.file.Files;
import java.nio.file.Paths;


public class KafkaProducerApplication  {
	
	public static void main(String[] args) throws Exception{
		String key = "any";
		int synch_received = 0, asynch_received = 0;
		
		
		//read file as string from file in producer location.
		// have all three files
		
		String file = "/Users/Dimosthenis/kafka-telsol/producerApp/src/main/java/io/confluent/developer/infrastructureup.json";
		String json = new String(Files.readAllBytes(Paths.get(file)));
		System.out.println(json);
		

	
		//String json_opa = "{\"fiber\": {\"type\": \"Gigabit-capable Passive Optical Network (GPON)\", \"mode\": \"Single Mode Fiber (ITU-T G.652)\", \"downstream\": {\"wavelength (nm)\": 1390, \"bandwidth (Gbps)\": 2.488}, \"upstream\": {\"wavelength (nm)\": 1310, \"bandwidth (Gbps)\": 1.244}, \"split_ratio\": \"1/32\"}, \"cable\": {\"type\": \"HDPE\", \"color\": \"black\", \"outside diameter (mm)\": 40, \"inside diameter (mm)\": 32}, \"OLT\": {\"loc\": [38.043447, 23.801385], \"address\": \"Dionisiou 138, Marousi\"}, \"OND\": {\"loc\": [38.052209, 23.804491], \"address\": \"Dionisiou 71, Marousi\"}, \"points\": [{\"num\": 1, \"type\": \"Point\", \"loc\": [38.045004, 23.802266], \"address\": \"Dionisiou 126-130, Marousi\", \"splitted\": false, \"ditch\": {\"type\": \"X1\", \"depth (mm)\": 400, \"width (mm)\": 120}}, {\"num\": 2, \"type\": \"Sewer\", \"loc\": [38.046464, 23.80212], \"address\": \"Nikos Kapetanidis Square 2-4, Marousi\", \"splitted\": false, \"sewer:\": {\"type\": \"Phi2\", \"length (mm)\": 600, \"width (mm)\": 600, \"depth (mm)\": 650, \"cable depth (mm)\": 250}, \"ditch\": {\"type\": \"X1\", \"depth (mm)\": 400, \"width (mm)\": 120}}, {\"num\": 3, \"type\": \"Point\", \"loc\": [38.047826, 23.803108], \"address\": \"Dionisiou 112, Marousi\", \"splitted\": false, \"ditch\": {\"type\": \"X1\", \"depth (mm)\": 400, \"width (mm)\": 120}}, {\"num\": 4,\"type\": \"Sewer\", \"loc\": [38.048989, 23.803479], \"address\": \"Dionisiou, Marousi\", \"splitted\": false, \"sewer:\": {\"type\": \"Phi2\", \"length (mm)\": 600, \"width (mm)\": 600, \"depth (mm)\": 650, \"cable depth (mm)\": 250}, \"ditch\": {\"type\": \"X1\", \"depth (mm)\": 400, \"width (mm)\": 120}}, {\"num\": 5, \"type\": \"Point\", \"loc\": [38.050978, 23.803669], \"address\": \"Marathonodromou 29-25, Marousi\", \"splitted\": false, \"ditch\": {\"type\": \"X1\", \"depth (mm)\": 400, \"width (mm)\": 120}}, {\"num\": 6, \"type\": \"OND\", \"loc\": [38.052209, 23.804491], \"address\": \"Dionisiou 71, Marousi\", \"splitted\": true}, {\"num\": 7, \"type\": \"Point\", \"loc\": [38.052712, 23.803283], \"address\": \"Dim. Gounari 18-29, Marousi\", \"splitted\": true, \"ditch\": {\"type\": \"X2\", \"depth (mm)\": 220, \"width (mm)\": 50}}, {\"num\": 8, \"type\": \"Sewer\", \"loc\": [38.053308, 23.801171], \"address\": \"Dim. Gounari 2-10, Marousi\", \"splitted\": true, \"sewer:\": {\"type\": \"Phi2\", \"length (mm)\": 600, \"width (mm)\": 600, \"depth (mm)\": 650, \"cable depth (mm)\": 250}, \"ditch\": {\"type\": \"X2\", \"depth (mm)\": 220, \"width (mm)\": 50}}, {\"num\": 9, \"type\": \"Point\", \"loc\": [38.051949, 23.80925], \"address\": \"Dim. Ralli 26, Marousi\", \"splitted\": true, \"ditch\": {\"type\": \"X2\", \"depth (mm)\": 220, \"width (mm)\": 50}}, {\"num\": 10, \"type\": \"Sewer\", \"loc\": [38.051649, 23.806984], \"address\": \"Dim. Gounari 61-57, Marousi\", \"splitted\": true, \"sewer:\": {\"type\": \"Phi2\", \"length (mm)\": 600, \"width (mm)\": 600, \"depth (mm)\": 650, \"cable depth (mm)\": 250}, \"ditch\": {\"type\": \"X2\", \"depth (mm)\": 220, \"width (mm)\": 50}}, {\"num\": 11, \"type\": \"Point\", \"loc\": [38.054142, 23.805268], \"address\": \"Dionisiou 40-44, Marousi\", \"splitted\": true, \"ditch\": {\"type\": \"X2\", \"depth (mm)\": 220, \"width (mm)\": 50}}]}";
		//System.out.printf(json_opa);

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
							"\"fields\":[{\"name\":\"Submission\",\"type\":\"string\"}]}";
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
			
			System.out.printf("\n\nUsername: ");
			String username = myObj.nextLine();
			System.out.printf("\n Password: ");
			String password = myObj.nextLine();

			//String pass_new = String.valueOf(System.console().readPassword());
			//System.out.printf("Password before encryption: " + pass_new);

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

		
		GenericRecord avroRecord = new GenericData.Record(schema);
		avroRecord.put("Submission", json);
		ProducerRecord<Object, Object> new_record = new ProducerRecord<>("insertion", key, avroRecord);
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
