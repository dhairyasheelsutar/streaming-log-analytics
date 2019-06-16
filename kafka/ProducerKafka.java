import org.apache.kafka.clients.producer.*;
import java.util.Properties;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.common.serialization.StringSerializer;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class ProducerKafka {

    private static final int port = 4000;
    private ServerSocket server;

    public static void main(String[] args){
        new ProducerKafka().run();
    }

    public void run(){

        final Logger logger = LoggerFactory.getLogger(ProducerKafka.class);
        String bootstrapServers = "127.0.0.1:9092";
        String topic = "logs";
        String key = "node-server";

        try{
            server = new ServerSocket(port);
        }catch (IOException e){
            logger.info("Cannot listen on port 4000");
            System.exit(-1);
        }

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        while(true){
            try{
                logger.info("Waiting for connection");
                final Socket socket = server.accept();
                final InputStream inputStream = socket.getInputStream();
                final InputStreamReader streamReader = new InputStreamReader(inputStream);
                BufferedReader br = new BufferedReader(streamReader);
                String line = null;
                while ((line = br.readLine()) != null) {
                    if(!new ProducerKafka().validateJSON(line)){
                        continue;
                    }
                    System.out.println(line);
                    ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, line);
                    producer.send(record, new Callback() {
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if(e != null){
                                logger.info("Some error");
                            }
                        }
                    });
                }
            }catch (IOException e){
                logger.info("Accept failed -1");
                System.exit(-1);
            }

            producer.flush();
            producer.close();
        }
    }

    private boolean validateJSON(String jsonString){
        JSONObject obj = new JSONObject(jsonString);
        String[] ip = obj.getString("IP").split("\\.");
        return ip.length == 4;
    }

}
