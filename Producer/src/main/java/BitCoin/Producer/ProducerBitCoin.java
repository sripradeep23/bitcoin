package BitCoin.Producer;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;

import com.neovisionaries.ws.client.WebSocket;
import com.neovisionaries.ws.client.WebSocketAdapter;
import com.neovisionaries.ws.client.WebSocketFactory;

public class ProducerBitCoin
{
	private final static String TOPIC = "bitcoin";
	private final static String BOOTSTRAP_SERVERS ="localhost:9092";
    public static void main(String[] args) throws Exception
    {
    	
    	final Producer<Long, String> prod = createProducer();
        new WebSocketFactory()
            .createSocket("wss://ws.blockchain.info/inv")
            .addListener(new WebSocketAdapter() {
                @Override
                public void onTextMessage(WebSocket ws, String message) {
                    // Received a response. Print the received message.
                	JSONObject jobj = new JSONObject(message);
                	System.out.println(jobj.toString());
                    ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(TOPIC,jobj.toString());
                    prod.send(record);
                }
            })
            .connect()
            .sendText("{\"op\":\"unconfirmed_sub\"}");
    }
    
    @SuppressWarnings("unchecked")
	private static Producer<Long, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        return new KafkaProducer(props);
    }
}