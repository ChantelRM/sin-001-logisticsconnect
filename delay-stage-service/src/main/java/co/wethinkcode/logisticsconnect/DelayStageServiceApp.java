package co.wethinkcode.logisticsconnect;

import io.javalin.Javalin;
import co.wethinkcode.logisticsconnect.mq.MqConfig;
import org.apache.activemq.ActiveMQConnectionFactory;
import javax.jms.*;
import java.time.Instant;
import java.util.*;

public class DelayStageServiceApp {
    private static Session mqSession;
    private static MessageProducer producer;
    private static Map<String,Integer> hubStages = new ConcurrentHashMap<>();

    private static void createMq() throws JMSException{
        ConnectionFactory factory = new ActiveMQConnectionFactory(MqConfig.BROKER_URL);
        Connection connection = factory.createConnection();
        connection.start();

        mqSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = mqSession.createTopic(MqConfig.TOPIC);
        producer = mqSession.createProducer(topic);
    }

    private static void publishStage(String hubId, int stageNo) throws JMSException{
        String json = String.format(
                "{\"hubId\":\"%s\",\"stage\":%d,\"timestamp\":\"%s\"}",
                hubId, stageNo, Instant.now().toString()
        );

        TextMessage message = mqSession.createTextMessage(json);
        producer.send(message);
    }

    public static void main(String[] args) {
        createMq();
        Javalin app = Javalin.create().start(7052);

        app.get("/health", ctx -> ctx.result("OK"));
        app.get("/delay-stage/{hubId}" , ctx -> {
            String id = ctx.pathParam("hubId");
            Integer stage = hubStages.get(id);

            if(stage== null){
                ctx.status(404);
            }
            else{
                Map<String,Object> response = new LinkedHashMap<>();
                response.put("hubId", id);
                response.put("stage",stage);

                ctx.json(response);
            }
        });
        app.post("/delay-stage/{hubId}" , ctx -> {
            String id= ctx.pathParam("hubId");

            Map<String, Object> body = ctx.bodyAsClass(Map.class);
            int stage = (Integer) body.get("stage");

            hubStages.put(id,stage);

            publishStage(id,stage);

            ctx.status(200);
        });
    }
}
