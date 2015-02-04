import java.util.*;
 
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
 
public class InfiniteKafkaProducer { 
    public static void main(String[] args) {
	String brokerList = args[0];
	String topic = args[1];
        Random rnd = new Random();
 
        Properties props = new Properties();
        props.put("metadata.broker.list", brokerList);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //props.put("partitioner.class", "example.producer.SimplePartitioner");
        //props.put("request.required.acks", "1");
 
        ProducerConfig config = new ProducerConfig(props);
 
        Producer<String, String> producer = new Producer<String, String>(config);
 
        while (true)
	{
	       	try{
	       		long runtime = new Date().getTime();  
               		String ip = "192.168.2." + rnd.nextInt(255); 
               		String msg = runtime + ",www.example.com," + ip; 
               		KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, ip, msg);
               		producer.send(data);
		}
	       	catch (Exception e){
			producer.close();
			System.exit(1);
		}
        }
    }
}
