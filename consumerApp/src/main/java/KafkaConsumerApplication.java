package io.confluent.examples.clients.basicavro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;


import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;


import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import java.time.Duration;

import java.nio.file.Files;
import java.nio.file.Paths;

import java.util.Arrays;
import java.util.Random;
import java.util.Properties;
import java.util.Collections;



public class KafkaConsumerApplication {

	//private static final String TOPIC = "transactions";

    @SuppressWarnings("InfiniteLoopStatement")
	public static void main(final String[] args){
/*
		final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-payments");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true); 

        try (final KafkaConsumer<String, Payment> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            while (true) {
                final ConsumerRecords<String, Payment> records = consumer.poll(100);
                for (final ConsumerRecord<String, Payment> record : records) {
                    final String key = record.key();
                    final Payment value = record.value();
                    System.out.printf("key = %s, value = %s%n", key, value);
                }
            }

        }*/
		
		Properties props_producer = new Properties();
		Properties props_consumer = new Properties();
		props_producer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props_producer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
		props_producer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
		props_producer.put("schema.registry.url", "http://localhost:8081");
		
		props_consumer.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props_consumer.put(ConsumerConfig.GROUP_ID_CONFIG, "insertion_group");
		props_consumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props_consumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
		props_consumer.put("schema.registry.url", "http://localhost:8081");
		props_consumer.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		
		KafkaProducer producer = new KafkaProducer(props_producer);
		final Consumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(props_consumer);
		
		

		String topic = "insertion";
		consumer.subscribe(Arrays.asList(topic));

		String ackSchema = "{\"type\":\"record\"," +
							"\"name\":\"myrecord\"," +
							"\"fields\":[{\"name\":\"ACK_Message\",\"type\":\"string\"}]}";
		

		int messages_produced = 0, synch_received = 0, asynch_received = 0;
		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(ackSchema);

        String producer_key, status, running_status = "up";

		//infinte poll loop
		while(true) {
            ConsumerRecords<String, GenericRecord> records = consumer.poll(100); // 100 is how long the poll with block if no data
			for (ConsumerRecord<String, GenericRecord> record : records) {
                System.out.printf("\n\n\n\n\noffset = %d, key = %s, value =:= %s\n", record.offset(), record.key(), record.value());
                producer_key = record.key();


                GenericRecord avroRecord = new GenericData.Record(schema);
				avroRecord.put("ACK_Message", " Consumed Message from " + producer_key + " % Synch %");

                ProducerRecord<Object, Object> producerRecord_synch =
                    new ProducerRecord<>("ack", producer_key, avroRecord);
                producer.send(producerRecord_synch);
                producer.flush();

                if(producer_key.toString().equals("admin")) {
                    running_status = "down";
                    break;
                }
                //sleep for 10 seconds
                try
                {
                    Thread.sleep(3000);
                }
                catch(InterruptedException ex)
                {
                    Thread.currentThread().interrupt();
                }
                
                // do something
				if(producer_key.toString().equals("WRONG")){
					status = "Error:\nType: Fiber Wire Discontinuity\nLocation: 10, [38.043908, 23.816087], Ari Fakinou 2-68, Marousi.\nComments: Closest point to Fiber Wire line: 11, [38.042463, 23.815143], Sorou 71, Marousi.";
				}
				else {
					status = "Successfully Completed";
				}
                
				if (status == "Successfully Completed") {
					ProducerRecord<Object, GenericRecord> infrastructure_record =
						new ProducerRecord<>("infrastructure_data", producer_key, record.value());
					producer.send(infrastructure_record);
					producer.flush();
				}

                GenericRecord avroRecord_asynch = new GenericData.Record(schema);
				avroRecord_asynch.put("ACK_Message", "Finalized Message " + producer_key + " % Asynch % with Status: " + status);
                
                ProducerRecord<Object, Object> producerRecord_asynch =
                    new ProducerRecord<>("ack", producer_key, avroRecord_asynch);
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
