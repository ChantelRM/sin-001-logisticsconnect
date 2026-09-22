package co.wethinkcode.logisticsconnect;

import io.javalin.Javalin;
import co.wethinkcode.logisticsconnect.mq.MqConfig;
import org.apache.activemq.ActiveMQConnectionFactory;
import javax.jms.*;
import java.util.*;
import com.fasterxml.jackson.core.databind.*;
import com.fasterxml.jackson.core.type.TypeReference;

public class TransitServiceApp {
    private static final Map<String, Integer> updatedHubStages = new ConcurrentHashMap<>();

    private static void subscribe() throws JMSException, IOException{
        ConnectionFactory factory = new ActiveMQConnectionFactory(MqConfig.BROKER_URL);
        Connection connection = factory.createConnection();
        connection.start();

        Session mqSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = mqSession.createTopic(MqConfig.TOPIC);

        MessageConsumer consumer = mqSession.createConsumer(topic);
        consumer.setMessageListener(message ->
                {
                    try{
                        if(message instanceof TextMessage textMessage){
                            String json = textMessage.getText();

                            ObjectMapper mapper = new ObjectMapper();
                            Map<String, Object> parsed = mapper.readValue(json, new TypeRefernce<Map<String,Object>>() {});

                            String id= (String) parsed.get("hubId");
                            Integer stage =(Integer) parsed.get("stage");

                            updatedHubStages.put(id,stage);

                        }
                    } catch(JMSException e) {
                        e.printStackTrace();
                    }
                }
         );

    }

    public static void main(String[] args) throws IOException, JMSException{
        subscribe();
        Javalin app = Javalin.create().start(7053);

        app.get("/health", ctx -> ctx.result("OK"));

        // TODO (Calculates estimated arrival windows based on hub and delay stage.)
        // Add domain endpoints for transit-service here.
    }
}

// MQ TODO: subscribes to ActiveMQ topic MqConfig.TOPIC at MqConfig.BROKER_URL (see co.wethinkcode.logisticsconnect.mq.MqConfig)
