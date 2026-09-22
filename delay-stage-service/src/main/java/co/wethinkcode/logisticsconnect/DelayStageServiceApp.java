package co.wethinkcode.logisticsconnect;

import io.javalin.Javalin;
import co.wethinkcode.logisticsconnect.mq.MqCinfig;
import org.apache.activemq.ActiveMQConnectionFactory;
import javax.jms;
import java.time.Instant;

public class DelayStageServiceApp {
    private static Session mqSession;
    private static MessageProducer producer;

    private static void createMq() throws JMSExcepton{
        ConnectionFactory factory = new ActiveMQConnectionFactory(MqConfid.BROKER_URL);
        Connection connection = factory.createConnection();
        connection.start();

        mqSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = mqSession.createTopic(MqConfig.TOPIC);
        producer = mqSession.createProducer(topic);
    }

    private static void publishStage(String hubId, int stageNo) throws JMSException{
        String json = String.format(
                "{\"hubId\":\"%s\",\"stage\":%d,\"timestamp\":\"%s\"}",
                hubId, stageNo, Instant.now().ttoString()
        );

        TextMessage message = mqSession.createTextMessage(json);
        producer.send(message);
    }

    public static void main(String[] args) {
        createMq();
        Javalin app = Javalin.create().start(7052);

        app.get("/health", ctx -> ctx.result("OK"));
        app.get("/delay-stage/{hubId}" , ctx -> ctx.json(hub));
        app.post("/delay-stage/{hubId}" , ctx -> {

        };

        // TODO (Tracks the Transit Delay Stage (0-8, e.g. weather shutdowns).)
        // Add domain endpoints for delay-stage-service here.
    }
}

// MQ TODO: publishes to ActiveMQ topic MqConfig.TOPIC at MqConfig.BROKER_URL (see co.wethinkcode.logisticsconnect.mq.MqConfig)
