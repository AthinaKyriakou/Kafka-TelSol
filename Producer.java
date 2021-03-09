package mykafka;



import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class ty { // main class
	public static void main(String args[]) { // main function
		
		//properties for producer
		//serializer: convert object to stream of bytes for purpose of transmission
		//kafka offers serialize, deserialize only for:
		//string, int, long, double
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092"); //cs of ip addresses for kafka broker
		props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer"); // convert data to bytestream
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		
		
		
		//create producer
		Producer<Integer, String> producer = new KafkaProducer<Integer, String>(props);
		
		//unboxingbigdata == name of TOPIC
		//send messages to my - topic
		for(int i =0; i < 20; i++) {
			ProducerRecord<Integer, String> producerRecord = new ProducerRecord<Integer, String>("test01", i, "Test Message #" + Integer.toString(i));
			producer.send(producerRecord);
		}
		
		//close producer
		producer.close();
	}

}
