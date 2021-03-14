package io.confluent.examples.clients.basicavro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.errors.SerializationException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import java.util.Properties;
import java.util.Arrays;
import java.util.Random;


import java.io.UnsupportedEncodingException;
import java.security.AlgorithmParameters;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import javax.crypto.KeyGenerator;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

import java.nio.file.Files;
import java.nio.file.Paths;
//////////////////////////////////////////////////////////////////////////////////////////////////

public class KafkaProducerApplication  {
	
	public static String generateKey(int n) throws NoSuchAlgorithmException {
		KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
		keyGenerator.init(n);
		SecretKey key = keyGenerator.generateKey();
		String encodedKey = Base64.getEncoder().encodeToString(key.getEncoded());
		return encodedKey;
	}

	private static SecretKeySpec createSecretKey(char[] password, byte[] salt, int iterationCount, int keyLength) throws NoSuchAlgorithmException, InvalidKeySpecException {
        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA512");
        PBEKeySpec keySpec = new PBEKeySpec(password, salt, iterationCount, keyLength);
        SecretKey keyTmp = keyFactory.generateSecret(keySpec);
        return new SecretKeySpec(keyTmp.getEncoded(), "AES");
    }

    private static String encrypt(String property, SecretKeySpec key) throws GeneralSecurityException, UnsupportedEncodingException {
        Cipher pbeCipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        pbeCipher.init(Cipher.ENCRYPT_MODE, key);
        AlgorithmParameters parameters = pbeCipher.getParameters();
        IvParameterSpec ivParameterSpec = parameters.getParameterSpec(IvParameterSpec.class);
        byte[] cryptoText = pbeCipher.doFinal(property.getBytes("UTF-8"));
        byte[] iv = ivParameterSpec.getIV();
        return base64Encode(iv) + ":" + base64Encode(cryptoText);
    }

    private static String base64Encode(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }

    private static String decrypt(String string, SecretKeySpec key) throws GeneralSecurityException, IOException {
        String iv = string.split(":")[0];
        String property = string.split(":")[1];
        Cipher pbeCipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        pbeCipher.init(Cipher.DECRYPT_MODE, key, new IvParameterSpec(base64Decode(iv)));
        return new String(pbeCipher.doFinal(base64Decode(property)), "UTF-8");
    }

    private static byte[] base64Decode(String property) throws IOException {
        return Base64.getDecoder().decode(property);
    }	
	@SuppressWarnings("InfiniteLoopStatement")
	public static void main(final String[] args) throws Exception{

		//System.out.printf("\n\n\n MY SPECIAL KEY IS :" + generateKey(192) + "\n\n\n\n");
		String special_key = "nL0ZAIlgp042s0J/evM47HdVGeqvpBaq";
		int synch_received = 0, asynch_received = 0;
		


		/*public class KafkaProducerApplication  {

		private static final String TOPIC = "insertion";

		@SuppressWarnings("InfiniteLoopStatement")
		public static void main(final String[] args){

			//dummy producer to check Avro schema implementation
			System.out.println("Starting dummy producer to check Avro schema implementation");
			
			try (KafkaProducer<String, Payment> producer = new KafkaProducer<String, Payment>(props)) {

				for (long i = 0; i < 10; i++) {
					final String orderId = "id" + Long.toString(i);
					final Payment payment = new Payment(orderId, 1000.00d);
					final ProducerRecord<String, Payment> record = new ProducerRecord<String, Payment>(TOPIC, payment.getId().toString(), payment);
					producer.send(record);
					Thread.sleep(1000L);
				}

				producer.flush();
				System.out.printf("Successfully produced 10 messages to a topic called %s%n", TOPIC);

			} catch (final SerializationException e) {
				e.printStackTrace();
			} catch (final InterruptedException e) {
				e.printStackTrace();
			}*/

	
		//String json_opa = "{\"fiber\": {\"type\": \"Gigabit-capable Passive Optical Network (GPON)\", \"mode\": \"Single Mode Fiber (ITU-T G.652)\", \"downstream\": {\"wavelength (nm)\": 1390, \"bandwidth (Gbps)\": 2.488}, \"upstream\": {\"wavelength (nm)\": 1310, \"bandwidth (Gbps)\": 1.244}, \"split_ratio\": \"1/32\"}, \"cable\": {\"type\": \"HDPE\", \"color\": \"black\", \"outside diameter (mm)\": 40, \"inside diameter (mm)\": 32}, \"OLT\": {\"loc\": [38.043447, 23.801385], \"address\": \"Dionisiou 138, Marousi\"}, \"OND\": {\"loc\": [38.052209, 23.804491], \"address\": \"Dionisiou 71, Marousi\"}, \"points\": [{\"num\": 1, \"type\": \"Point\", \"loc\": [38.045004, 23.802266], \"address\": \"Dionisiou 126-130, Marousi\", \"splitted\": false, \"ditch\": {\"type\": \"X1\", \"depth (mm)\": 400, \"width (mm)\": 120}}, {\"num\": 2, \"type\": \"Sewer\", \"loc\": [38.046464, 23.80212], \"address\": \"Nikos Kapetanidis Square 2-4, Marousi\", \"splitted\": false, \"sewer:\": {\"type\": \"Phi2\", \"length (mm)\": 600, \"width (mm)\": 600, \"depth (mm)\": 650, \"cable depth (mm)\": 250}, \"ditch\": {\"type\": \"X1\", \"depth (mm)\": 400, \"width (mm)\": 120}}, {\"num\": 3, \"type\": \"Point\", \"loc\": [38.047826, 23.803108], \"address\": \"Dionisiou 112, Marousi\", \"splitted\": false, \"ditch\": {\"type\": \"X1\", \"depth (mm)\": 400, \"width (mm)\": 120}}, {\"num\": 4,\"type\": \"Sewer\", \"loc\": [38.048989, 23.803479], \"address\": \"Dionisiou, Marousi\", \"splitted\": false, \"sewer:\": {\"type\": \"Phi2\", \"length (mm)\": 600, \"width (mm)\": 600, \"depth (mm)\": 650, \"cable depth (mm)\": 250}, \"ditch\": {\"type\": \"X1\", \"depth (mm)\": 400, \"width (mm)\": 120}}, {\"num\": 5, \"type\": \"Point\", \"loc\": [38.050978, 23.803669], \"address\": \"Marathonodromou 29-25, Marousi\", \"splitted\": false, \"ditch\": {\"type\": \"X1\", \"depth (mm)\": 400, \"width (mm)\": 120}}, {\"num\": 6, \"type\": \"OND\", \"loc\": [38.052209, 23.804491], \"address\": \"Dionisiou 71, Marousi\", \"splitted\": true}, {\"num\": 7, \"type\": \"Point\", \"loc\": [38.052712, 23.803283], \"address\": \"Dim. Gounari 18-29, Marousi\", \"splitted\": true, \"ditch\": {\"type\": \"X2\", \"depth (mm)\": 220, \"width (mm)\": 50}}, {\"num\": 8, \"type\": \"Sewer\", \"loc\": [38.053308, 23.801171], \"address\": \"Dim. Gounari 2-10, Marousi\", \"splitted\": true, \"sewer:\": {\"type\": \"Phi2\", \"length (mm)\": 600, \"width (mm)\": 600, \"depth (mm)\": 650, \"cable depth (mm)\": 250}, \"ditch\": {\"type\": \"X2\", \"depth (mm)\": 220, \"width (mm)\": 50}}, {\"num\": 9, \"type\": \"Point\", \"loc\": [38.051949, 23.80925], \"address\": \"Dim. Ralli 26, Marousi\", \"splitted\": true, \"ditch\": {\"type\": \"X2\", \"depth (mm)\": 220, \"width (mm)\": 50}}, {\"num\": 10, \"type\": \"Sewer\", \"loc\": [38.051649, 23.806984], \"address\": \"Dim. Gounari 61-57, Marousi\", \"splitted\": true, \"sewer:\": {\"type\": \"Phi2\", \"length (mm)\": 600, \"width (mm)\": 600, \"depth (mm)\": 650, \"cable depth (mm)\": 250}, \"ditch\": {\"type\": \"X2\", \"depth (mm)\": 220, \"width (mm)\": 50}}, {\"num\": 11, \"type\": \"Point\", \"loc\": [38.054142, 23.805268], \"address\": \"Dionisiou 40-44, Marousi\", \"splitted\": true, \"ditch\": {\"type\": \"X2\", \"depth (mm)\": 220, \"width (mm)\": 50}}]}";
		//System.out.printf(json_opa);

		final Properties props_producer = new Properties();
		final Properties props_consumer = new Properties();
		final Properties login_props_consumer = new Properties();
		props_producer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props_producer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
		props_producer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
		props_producer.put("schema.registry.url", "http://localhost:8081");
		
		final Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
		props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
		

		props_consumer.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props_consumer.put(ConsumerConfig.GROUP_ID_CONFIG, "group_paroxos");
		props_consumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props_consumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
		props_consumer.put("schema.registry.url", "http://localhost:8081");
		props_consumer.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		login_props_consumer.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		login_props_consumer.put(ConsumerConfig.GROUP_ID_CONFIG, "group_log");
		login_props_consumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		login_props_consumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
		login_props_consumer.put("schema.registry.url", "http://localhost:8081");
		login_props_consumer.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		
		KafkaProducer producer = new KafkaProducer(props_producer);
		// KafkaProducer producer_insertion = new KafkaProducer(props);
		final Consumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(props_consumer);
		final Consumer<String, Integer> login_consumer = new KafkaConsumer<String, Integer>(login_props_consumer);
		
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
		int login = 0, validation_reply = 0;
		String logged_in_user = "";
		String key = "";

		while(login != 1){
			
			System.out.printf("\n Username: ");
			String username = String.valueOf(System.console().readLine());
			System.out.printf("\n Password: ");

			String password = String.valueOf(System.console().readPassword());
			System.out.println("Password before encryption: " + password + " and username is : " + username);
			key = username;
			/////////////////////////////////////////////
			

			// The salt (probably) can be stored along with the encrypted data
			byte[] salt = new String("12345678").getBytes();

			// Decreasing this speeds down startup time and can be useful during testing, but it also makes it easier for brute force attackers
			int iterationCount = 40000;
			// Other values give me java.security.InvalidKeyException: Illegal key size or default parameters
			int keyLength = 128;
			SecretKeySpec en_key = createSecretKey(special_key.toCharArray(),
					salt, iterationCount, keyLength);

			String originalPassword = password;
			//System.out.println("Original password: " + originalPassword);
			String encryptedPassword = encrypt(originalPassword, en_key);
			//System.out.println("Encrypted password: " + encryptedPassword);
			//String decryptedPassword = decrypt(encryptedPassword, en_key);
			//System.out.println("Decrypted password: " + decryptedPassword);
			///////////////////////////////////////////

			System.out.println("Password after encryption: " + encryptedPassword + " and username is : " + username + "\n\n\n\n");

			GenericRecord login_avroRecord = new GenericData.Record(login_schema);
			login_avroRecord.put("Username", username);
			login_avroRecord.put("Password", encryptedPassword); // change password to encrypted
			ProducerRecord<Object, Object> login_record = new ProducerRecord<>("user_data", key, login_avroRecord);
			producer.send(login_record);
			producer.flush();
			validation_reply = 0;
			while(validation_reply == 0){
				ConsumerRecords<String, Integer> records = login_consumer.poll(100);
				for(ConsumerRecord<String, Integer> validation_record : records) {
					if((validation_record.key()).toString().equals(key.toString())){
						login = validation_record.value();
						System.out.printf("\n\nVALUES READ" + validation_record.value()+" login : " + login + "\n\n");
						validation_reply = 1;
					}
				}
			}
			
			if(login == 0){
				System.out.printf("Login Failed, Please Check you Username and Password and Retry \n\n\n");
			}
			else{
				logged_in_user = username;
			}
		}
		key = logged_in_user;


		System.out.println("\n\n\n Please Select Data Info to Send: ");
		System.out.println("	(1) : infrastructureright.json");
		System.out.println("	(2) : infrastructureright_fakinou.json");
		System.out.println("	(3) : infrastructureup.json\n");
		
		//read input
		String user_input = "", file = "";
		int pending = 0, number_input = 0;

		while(pending == 0){
			System.out.println("Option:		");
			user_input = String.valueOf(System.console().readLine());
			try {
				number_input = Integer.parseInt(user_input);
				System.out.println("Input String converted to: " + number_input + "\n");
			} catch (NumberFormatException e) {
				System.out.println("Please Enter a Valid Number");
			}
			if ((number_input >= 1) && (number_input <= 3)){
				pending = 1;
			}
			else {
				System.out.println("Please choose a specific option");
			}
		}

		switch(number_input){
			case 1:
				file = "/media/panso/Data/HDD_Documents/HMMY/Roh_L/9o_Systems/Παραδοτέο_4/kafka-telsol/producerApp/src/main/java/infrastructureright.json";
				break;
			case 2:
				file = "/media/panso/Data/HDD_Documents/HMMY/Roh_L/9o_Systems/Παραδοτέο_4/kafka-telsol/producerApp/src/main/java/infrastructureright_fakinou.json";
				break;
			case 3:
				file = "/media/panso/Data/HDD_Documents/HMMY/Roh_L/9o_Systems/Παραδοτέο_4/kafka-telsol/producerApp/src/main/java/infrastructureup.json";
				break;

		}
		
		String json = new String(Files.readAllBytes(Paths.get(file)));
		System.out.println(json);
		////////////// READING JSON FROM FILE AND CREATING SCHEMA
		// CHANGE producer -- > producer_insertion
		
		GenericRecord avroRecord = new GenericData.Record(schema);
		avroRecord.put("Submission", json);
		ProducerRecord<Object, Object> new_record = new ProducerRecord<>("insertion", key, avroRecord); // encryptedPassword = key
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
