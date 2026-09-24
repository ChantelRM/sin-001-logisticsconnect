package co.wethinkcode.logisticsconnect;

import io.javalin.Javalin;
import co.wethinkcode.logisticsconnect.mq.MqConfig;
import org.apache.activemq.ActiveMQConnectionFactory;
import javax.jms.*;
import java.util.*;
import java.io.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.core.type.TypeReference;


public class AlertBotApp {
    private static final int STAGE_THRESHOLD=5;

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
                            Map<String, Object> parsed = mapper.readValue(json,
                                    new TypeReference<Map<String,Object>>() {});

                            String id= (String) parsed.get("hubId");
                            Integer stage =(Integer) parsed.get("stage");

                            if(stage !=null && stage >= STAGE_THRESHOLD){
                                System.out.println("Hub " + id + " has been delayed!!");
                            }

                        }
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                }
        );

    }

    public static void main(String[] args) throws IOException, JMSException {
        subscribe();
        Javalin app = Javalin.create().start(7054);

        app.get("/health", ctx -> ctx.result("OK"));

    }
}
