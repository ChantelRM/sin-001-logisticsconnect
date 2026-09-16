package co.wethinkcode.logisticsconnect;

import io.javalin.Javalin;
import java.net.http.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.*;
import java.net.URI;
import java.io.*;

public class HubServiceApp {
    private static Map<String,Map<String,Object>> fetchAllHubs(String httpUrl) throws IOException, InterruptedException{
        // call ingestion service somewhere here and return it
        HttpClient hubClient = HttpClient.newHttpClient();
        HttpRequest hubRequest = HttpRequest.newBuilder(URI.create(httpUrl)).GET().build();
        HttpResponse<String> response = hubClient.send(hubRequest, HttpResponse.BodyHandlers.ofString());

        ObjectMapper mapper = new ObjectMapper();
        List<Map<String, Object>> allHubs= mapper.readValue(response.body(),
                new TypeReference<List<Map<String,Object>>>() {});

        Map<String, Map<String, Object>> hubs = new HashMap<>();

        for(Map<String,Object> hub: allHubs){
            hubs.put((String) hub.get("hubId"),hub);
        }

        return hubs;
    }

    private static Map<String, Object> fetchHub(Map<String, Map<String, Object>> hubs, String id){
    // loop through all the hubs an dget by id
        return hubs.get(id.upperCase());
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
            Map<String, Object> hub = fetchHub(hubs,ctx.pathParam("hubID"));

            if(hub==null){
                ctx.status(404);
            }else{
            ctx.json(hub);
            }
        });
    }
}
