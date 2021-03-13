package io.confluent.developer;

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


public class UserDataValidatorApplication  {
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


	public static void main(String[] args) throws Exception{

		String special_key = "nL0ZAIlgp042s0J/evM47HdVGeqvpBaq";
		// The salt (probably) can be stored along with the encrypted data
		byte[] salt = new String("12345678").getBytes();

		// Decreasing this speeds down startup time and can be useful during testing, but it also makes it easier for brute force attackers
		int iterationCount = 40000;
		// Other values give me java.security.InvalidKeyException: Illegal key size or default parameters
		int keyLength = 128;
		SecretKeySpec en_key = createSecretKey(special_key.toCharArray(),
				salt, iterationCount, keyLength);

		Properties props_producer = new Properties();
		Properties props_consumer = new Properties();
		props_producer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props_producer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
		props_producer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
		props_producer.put("schema.registry.url", "http://localhost:8081");
		
		props_consumer.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props_consumer.put(ConsumerConfig.GROUP_ID_CONFIG, "group-login");
		props_consumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props_consumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
		props_consumer.put("schema.registry.url", "http://localhost:8081");
		props_consumer.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		KafkaProducer producer = new KafkaProducer(props_producer);
		final Consumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(props_consumer);
		
		consumer.subscribe(Arrays.asList("user_data"));
		
		String correct_username = "Dimos";
		String correct_password = "Panos";
		int login = 0;

		

		while(true){
			ConsumerRecords<String, GenericRecord> records = consumer.poll(100);
			for(ConsumerRecord<String, GenericRecord> record : records) {
				System.out.printf("reading data: %s", record.value());
				String username = record.value().get("Username").toString();
				String password = record.value().get("Password").toString();
				System.out.printf("username: %s password : %s", username, password);
				String key = record.key();

				/////////////////////////////////////////////
			
				String originalPassword = password;
				System.out.println("Original password: " + originalPassword);
				String decryptedPassword = decrypt(originalPassword, en_key);
				System.out.println("Decrypted password: " + decryptedPassword);
				///////////////////////////////////////////

				if((correct_username.equals(username)) && (correct_password.equals(decryptedPassword))){
					login = 1;
				}
				else{
					login = 0;
				}
				////////////////////////////////////////////
				//produce
				ProducerRecord<String, Integer> login_record = new ProducerRecord<>("validated_user_data", key, login);
				producer.send(login_record);
				producer.flush();

			}
		}

		
	}
}
