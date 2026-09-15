package co.wethinkcode.logisticsconnect;

import io.javalin.Javalin;
import com.opencsv.*;
import java.io.FileReader;
import java.util.*;
import java.nio.charset.StandardCharsets;

public class IngestionServiceApp {
    private static List<Map<String,Object>> cleanCsv(String path){
        List<String[]> rawRecords;

        try(InputStream in = IngestionService.class.getResourceAsStream(resourcePath));

        CVSReader csvReader = new CSVReader(new InputStreamReader(in,StandardCharsets.UFT_8)){
            List<String[]> allRows = csvReader.readAll();

            rawRecords = allRows.subList(1,allRows.size());
        }
        // fix id's

        // fix provinces

        //fix centers

        //fix active boolean

    }

    public static void main(String[] args) {
        Javalin app = Javalin.create().start(7050);

        app.get("/health", ctx -> ctx.result("OK"));
        app.get("/hubs"), ctx -> ctx.json(hubs)

        // TODO: read and clean src/main/resources/hubs-global.csv (hubs, sorting centers, regional districts data —
        // trim whitespace, fix casing, normalize dates/booleans) and expose the
        // cleaned records here for the other services to consume.
    }
}
