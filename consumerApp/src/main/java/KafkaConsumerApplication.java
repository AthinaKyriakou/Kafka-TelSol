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

import org.json.JSONArray;
import org.json.JSONObject;

public class KafkaConsumerApplication {

	//private static final String TOPIC = "transactions";
	private static final String TOPIC = "insertion_db";
	private static final String TOPICP = "insertion_db_points";

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
		Properties props_db = new Properties();
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

		props_db.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props_db.put(ProducerConfig.ACKS_CONFIG, "all");
		props_db.put(ProducerConfig.RETRIES_CONFIG, 0);
		props_db.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props_db.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
		props_db.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
		
		
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
			//Integer count = 1;
            ConsumerRecords<String, GenericRecord> records = consumer.poll(100); // 100 is how long the poll with block if no data
			for (ConsumerRecord<String, GenericRecord> record : records) {
			//	if (count == 1) {
			//		count = 2;
			//		continue;
			//	}
                //System.out.printf("\n\n\n\n\noffset = %d, key = %s, value =:= %s\n", record.offset(), record.key(), record.value());
                //System.out.printf("\n%s\n", record.value().get("Submission").getClass());
                //System.out.printf("\n%s\n", record.value().get("Submission").toString().getClass());
                producer_key = record.key();

                /////////////////////////////////////////////////////////////////////////////////
                JSONArray array = new JSONArray("[" + record.value().get("Submission").toString() + "]");  
                JSONObject object = array.getJSONObject(0);
				//for(int i=0; i < array.length(); i++) {  
				//	JSONObject object = array.getJSONObject(i);
					//System.out.println(object);
				//	JSONObject fiber = object.getJSONObject("fiber");
				//	System.out.println(fiber.getString("type"));  
				//}
				////////////////////////////////////////////////////////////////////////////////

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
                
                // update database
				if (status == "Successfully Completed") {
					JSONObject fiber;
					String infrastructure_id = new String("NULL");
					try (KafkaProducer<String, Payment> producer_db = new KafkaProducer<String, Payment>(props_db)) {
				
						fiber = object.getJSONObject("fiber");
						infrastructure_id = String.valueOf(fiber.getInt("id"));
						String infrastructure_type = fiber.getString("type");
						String infrastructure_mode = fiber.getString("mode");
						JSONObject downstream = fiber.getJSONObject("downstream");
						String downstream_wavelength = String.valueOf(downstream.getInt("wavelength (nm)"));
						//System.out.printf("\n%s\n", downstream);
						//System.out.printf("\n%s\n", downstream.get("bandwidth (Gbps)"));
						
						String downstream_bandwidth = String.valueOf(downstream.get("bandwidth (Gbps)"));
						JSONObject upstream = fiber.getJSONObject("upstream");
						String upstream_wavelength = String.valueOf(upstream.getInt("wavelength (nm)"));
						String upstream_bandwidth = String.valueOf(upstream.get("bandwidth (Gbps)"));
						String split_ratio = fiber.getString("split_ratio");

						JSONObject cable = object.getJSONObject("cable");
						String cable_type = cable.getString("type");
						String cable_color = cable.getString("color");
						String cable_out_diam = String.valueOf(cable.getInt("outside diameter (mm)"));
						String cable_in_diam = String.valueOf(cable.getInt("inside diameter (mm)"));

						JSONObject olt = object.getJSONObject("OLT");
						String olt_loc = olt.get("loc").toString();
						String olt_latitude = olt_loc.substring(1, 9);
						String olt_longitude = olt_loc.substring(11, 19);
						String olt_address = olt.getString("address");

						JSONObject splitter = object.getJSONObject("Splitter");
						String splitter_loc = splitter.get("loc").toString();
						String splitter_latitude = splitter_loc.substring(1, 9);
						String splitter_longitude = splitter_loc.substring(11, 19);
						String splitter_address = splitter.getString("address");

						Payment infrastructure = new Payment(infrastructure_id, infrastructure_type, infrastructure_mode, downstream_wavelength, downstream_bandwidth, upstream_wavelength, upstream_bandwidth, split_ratio, cable_type, cable_color, cable_out_diam, cable_in_diam, olt_latitude, olt_longitude, olt_address, splitter_latitude, splitter_longitude, splitter_address);
						ProducerRecord<String, Payment> db_record = new ProducerRecord<String, Payment>(TOPIC, infrastructure.getId().toString(), infrastructure);
						producer_db.send(db_record);
						Thread.sleep(1000L);
						producer_db.flush();
						System.out.printf("Successfully delivered message to a topic called %s%n", TOPIC);
					}
					catch (final SerializationException e) {
						e.printStackTrace();
					} 
					catch (final InterruptedException e) {
						e.printStackTrace();
					}
					try (KafkaProducer<String, Point> producer_db_point = new KafkaProducer<String, Point>(props_db)) {
						// now for the points
						JSONArray points = new JSONArray(object.get("points").toString());
						//System.out.printf("\n%s\n", object.get("points").toString());
						for(int i=0; i < points.length(); i++) { 
							JSONObject point = points.getJSONObject(i);
							String point_id = infrastructure_id + "_" + String.valueOf(point.getInt("num"));
							String point_type = point.getString("type");
							String point_loc = point.get("loc").toString();
							String point_latitude = point_loc.substring(1, 9);
							String point_longitude = point_loc.substring(11, 19);
							String point_address = point.getString("address");
							String point_splitted = point.get("splitted").toString();
							String ditch_type, ditch_depth, ditch_width, sewer_type, sewer_length, sewer_depth, sewer_width, sewer_cable_depth;
							if (point_type.equals("Splitter")) {
								ditch_type = new String("NULL");
								ditch_depth = new String("NULL");
								ditch_width = new String("NULL");
							}
							else{
								JSONObject ditch = point.getJSONObject("ditch");
								ditch_type = ditch.getString("type");
								ditch_depth = String.valueOf(ditch.getInt("depth (mm)"));
								ditch_width = String.valueOf(ditch.getInt("width (mm)"));
							}
							if (point_type.equals("Sewer")) {
								JSONObject sewer = point.getJSONObject("sewer");
								sewer_type = sewer.getString("type");
								sewer_length = String.valueOf(sewer.getInt("length (mm)"));
								sewer_depth = String.valueOf(sewer.getInt("depth (mm)"));
								sewer_width = String.valueOf(sewer.getInt("width (mm)"));
								sewer_cable_depth = String.valueOf(sewer.getInt("cable depth (mm)"));
							}
							else{
								sewer_type = new String("NULL");
								sewer_length = new String("NULL");
								sewer_depth = new String("NULL");
								sewer_width = new String("NULL");
								sewer_cable_depth = new String("NULL");
							}
							Point mypoint = new Point(point_id, infrastructure_id, point_type, point_latitude, point_longitude, point_address, point_splitted, ditch_type, ditch_depth, ditch_width, sewer_type, sewer_length, sewer_depth, sewer_width, sewer_cable_depth);
							ProducerRecord<String, Point> db_record_point = new ProducerRecord<String, Point>(TOPICP, mypoint.getId().toString(), mypoint);
							producer_db_point.send(db_record_point);
							Thread.sleep(1000L);
							producer_db_point.flush();
							System.out.printf("Successfully delivered message to a topic called %s%n", TOPICP);
						}
					}

					catch (final SerializationException e) {
						e.printStackTrace();
					} 
					catch (final InterruptedException e) {
						e.printStackTrace();
					}

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
