package co.wethinkcode.logisticsconnect;

import io.javalin.Javalin;
import co.wethinkcode.logisticsconnect.mq.MqConfig;
import org.apache.activemq.ActiveMQConnectionFactory;
import javax.jms.*;
import java.util.*;
import java.io.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.core.type.TypeReference;
import java.net.http.*;
import java.net.URI;


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
                            Map<String, Object> parsed = mapper.readValue(json,
                                    new TypeReference<Map<String,Object>>() {});

                            String id= (String) parsed.get("hubId");
                            Integer stage =(Integer) parsed.get("stage");

                            updatedHubStages.put(id,stage);

                        }
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                }
         );

    }

    private static Map<String,Object> fetchHub(String hubId) throws IOException, InterruptedException{
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder(
                URI.create("http://localhost:7051/hubs/" + hubId))
                .GET.build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        if(response.statusCode()==404){
            return null;
        }

        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(response.body(),
                new TypeReference<Map<String,Object>>() {});
    }

    public static void main(String[] args) throws IOException, JMSException{
        subscribe();
        Javalin app = Javalin.create().start(7053);

        app.get("/health", ctx -> ctx.result("OK"));
        app.get("/eta/{hubId}" , ctx -> {
            String id = ctx.pathParam("hubId");
            Map<String,Object> hub = fetchHub(id);

            if(hub== null) {ctx.status(404); return;}

            int stage = updatedHubStages.getOrDefault(id,0);

            int baseEtaHours = 24;
            int delay = 2;

            int eta= baseEtaHours + (stage * delay);

            // TO-DO: BUILD RESPONSE
            Map<String,Object> response = new LinkedHashMap<>();
            response.put("hubId",id);
            response.put("province", hub.get("province"));
            response.put("stage",stage);
            response.put("estimatedArrivalHours", eta);
            ctx.json(response);
        });
    }
}

