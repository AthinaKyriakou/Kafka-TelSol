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
	
	public static void main(String[] args) throws Exception {
		
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
		
		

		String topic = "topic1";
		consumer.subscribe(Arrays.asList(topic));

		String message, key = "DEH";
		String userSchema = "{\"type\":\"record\"," +
							"\"name\":\"myrecord\"," +
							"\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}";
		int messages_produced = 0, synch_received = 0, asynch_received = 0;
		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(userSchema);

        String producer_key, status, running_status = "up";

		//infinte poll loop
		while(true) {
            ConsumerRecords<String, GenericRecord> records = consumer.poll(100); // 100 is how long the poll with block if no data
			for (ConsumerRecord<String, GenericRecord> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
                producer_key = record.key();

                GenericRecord avroRecord = new GenericData.Record(schema);
				avroRecord.put("f1", " Consumed Test Message from #" + producer_key + "with value : " + record.value() + "# SYCHRONOUSLY");

                ProducerRecord<Object, Object> producerRecord_synch =
                        new ProducerRecord<>("topic2", producer_key, avroRecord);
                producer.send(producerRecord_synch);
                producer.flush();

                if(producer_key == "admin") {
                    running_status = "down";
                    break;
                }
                //sleep for 3 seconds
                try
                {
                    Thread.sleep(3000);
                }
                catch(InterruptedException ex)
                {
                    Thread.currentThread().interrupt();
                }
                
                // do something
                status = "success";

                GenericRecord avroRecord_asynch = new GenericData.Record(schema);
				avroRecord_asynch.put("f1", "Finalized Test Message #" + producer_key + "# ASYNCHRONOUSLY with status: " + status);
                
                ProducerRecord<Object, Object> producerRecord_asynch =
                        new ProducerRecord<>("topic2", producer_key, avroRecord_asynch);
                producer.send(producerRecord_asynch);
                producer.flush();
                
            }
            if(running_status == "down")
                break;


            
			
		}


		//close producer if we ever want to terminate the while loop
		producer.close();
		consumer.close();

	}

}
