package co.wethinkcode.logisticsconnect;

import io.javalin.Javalin;
import java.net.http.*;
import com.fasterxml.jackson.datbind.*;
import com.fasterxml.jackson.core.*;

public class HubServiceApp {
    private static Map<String,Map<String,Object>> fetchAllHubs(String httpUrl) throws IOException, InterrupttedException{
        // call ingestion service somewhere here and return it
        HttpClient hubClient = HttpClient.newHttpClient();
        HttpRequest hubRequest = HttpRequest.newBuilder(URI.create(httpUrl)).GET().build();
        HttpResponse<String> response = hubClient.send(hubRequest, HttpResponse.BodyHandlers.ofString());

        ObjectMapper mapper = new ObjectMapper();
        Map<String, Map<String, Object>> allHubs= mapper.readValue(response.body(),
                new TypeReference<Map<String, Map<String,Object>>>() {});

        return allHubs;
    }

    private static Map<String, Object> fetchHub(String id){
    // loop through all the hubs an dget by id
        Map<String, Map<String, Object>> hubs = fetchAllHubs("http://localhost:7050/hubs");

        return hubs.get(id);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Javalin app = Javalin.create().start(7051);

        app.get("/health", ctx -> ctx.result("OK"));

        Map<String, Map<String,Object>> hubs = fetchAllHubs("http://localhost:7050/hubs");

        // TODO (Serves provinces and sorting centers (place-name source of truth).)
        // Add domain endpoints for hub-service here.
        app.get("/hubs", ctx -> ctx.json(hubs));
        app.get("/hubs/{hubID}", ctx -> {
            //find hub by id
            if(fetchHub({hubID})==null){
                ctx.status(404)
            }
        });
    }
}
