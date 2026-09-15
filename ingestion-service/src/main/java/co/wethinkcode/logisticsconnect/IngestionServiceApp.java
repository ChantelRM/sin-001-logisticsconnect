package co.wethinkcode.logisticsconnect;

import io.javalin.Javalin;
import com.opencsv.*;
import java.io.FileReader;
import java.util.*;
import java.nio.charset.StandardCharsets;

public class IngestionServiceApp {
    private static final Map<String, String> PROVINCES = Map.ofEntries(
            Map.entry("gauteng", "Gauteng"),
            Map.entry("western cape", "Western Cape"),
            Map.entry("eastern cape", "Eastern Cape"),
            Map.entry("kwazulu-natal", "KwaZulu-Natal"),
            Map.entry("kwa-zulu natal", "KwaZulu-Natal"),
            Map.entry("kwazulu natal", "KwaZulu-Natal"),
            Map.entry("free state", "Free State"),
            Map.entry("limpopo", "Limpopo"),
            Map.entry("north west", "North West"),
            Map.entry("mpumalanga", "Mpumalanga"),
            Map.entry("northern cape", "Northern Cape")
    );


    private static List<Map<String,Object>> cleanCsv(String path){
        List<String[]> rawRecords;

        try(InputStream in = IngestionService.class.getResourceAsStream(resourcePath));

        CVSReader csvReader = new CSVReader(new InputStreamReader(in,StandardCharsets.UFT_8)){
            List<String[]> allRows = csvReader.readAll();

            rawRecords = allRows.subList(1,allRows.size());
        }

        // Built first so rows with a blank province (H-508) can borrow one
        // from another row that shares the same sorting_center.
        Map<String, String> sortingCenterToProvince = buildProvinceLookup(rawRows);

        List<Map<String, Object>> cleaned = new ArrayList<>();
        for (String[] row : rawRows) {
            String hubId = normalizeHubId(row[0]);
            String sortingCenter = normalizeSortingCenter(row[2]);
            String province = normalizeProvince(row[1], sortingCenter, sortingCenterToProvince);
            Boolean active = normalizeActive(row[3]);

            Map<String, Object> hub = new LinkedHashMap<>();
            hub.put("hubId", hubId);
            hub.put("province", province);
            hub.put("sortingCenter", sortingCenter);
            hub.put("active", active);
            cleaned.add(hub);
        }

        return deduplicate(cleaned);
    }

    private static String normalizeHubId(String raw) {
        return raw.trim().toUpperCase();
    }

    private static String normalizeSortingCenter(String raw) {
        String collapsed = raw.trim().replaceAll("\\s+", " ");
        StringBuilder result = new StringBuilder();
        for (String word : collapsed.split(" ")) {
            if (word.isEmpty()) continue;
            if (result.length() > 0) result.append(" ");
            result.append(Character.toUpperCase(word.charAt(0)))
                    .append(word.substring(1).toLowerCase());
        }
        return result.toString();
    }

    private static Map<String, String> buildProvinceLookup(List<String[]> rawRows) {
        Map<String, String> lookup = new HashMap<>();
        for (String[] row : rawRows) {
            String rawProvince = row[1].trim();
            if (rawProvince.isEmpty()) continue;

            String canonicalProvince = CANONICAL_PROVINCES.get(rawProvince.toLowerCase());
            String sortingCenter = normalizeSortingCenter(row[2]);
            if (canonicalProvince != null) {
                lookup.put(sortingCenter, canonicalProvince);
            }
        }
        return lookup;
    }

    private static String normalizeProvince(String rawProvince, String normalizedSortingCenter,
                                            Map<String, String> sortingCenterToProvince) {
        String trimmed = rawProvince.trim();
        if (trimmed.isEmpty()) {
            // Missing province (e.g. H-508) — infer from another row with the same sorting center.
            return sortingCenterToProvince.get(normalizedSortingCenter);
        }
        return CANONICAL_PROVINCES.getOrDefault(trimmed.toLowerCase(), trimmed);
    }

    private static Boolean normalizeActive(String raw) {
        String value = raw.trim().toUpperCase();
        return switch (value) {
            case "Y", "YES", "1", "TRUE" -> true;
            case "N", "NO", "0", "FALSE" -> false;
            default -> null; // "unknown", "N/A", etc. — genuinely unknown, not a guess
        };
    }

    // Groups hubs describing the same real-world location (same province +
    // sorting center) and collapses each group to one record.
    //
    // Resolution rule: keep the lowest hub_id as the surviving ID. For a
    // conflicting `active` flag within the group, take a majority vote among
    // the non-null values; if it's a tie, use the value from the lowest hub_id.
    private static List<Map<String, Object>> deduplicate(List<Map<String, Object>> hubs) {
        Map<String, List<Map<String, Object>>> groups = new LinkedHashMap<>();
        for (Map<String, Object> hub : hubs) {
            String key = hub.get("province") + "|" + hub.get("sortingCenter");
            groups.computeIfAbsent(key, k -> new ArrayList<>()).add(hub);
        }

        List<Map<String, Object>> result = new ArrayList<>();
        for (List<Map<String, Object>> group : groups.values()) {
            group.sort(Comparator.comparing(h -> (String) h.get("hubId")));
            Map<String, Object> winner = group.get(0);

            int trueCount = 0, falseCount = 0;
            for (Map<String, Object> hub : group) {
                Boolean active = (Boolean) hub.get("active");
                if (Boolean.TRUE.equals(active)) trueCount++;
                else if (Boolean.FALSE.equals(active)) falseCount++;
            }

            Boolean resolvedActive;
            if (trueCount > falseCount) resolvedActive = true;
            else if (falseCount > trueCount) resolvedActive = false;
            else resolvedActive = (Boolean) winner.get("active"); // tie -> lowest hub_id's value

            Map<String, Object> merged = new LinkedHashMap<>(winner);
            merged.put("active", resolvedActive);
            result.add(merged);
        }
        return result;
    }

    public static void main(String[] args) {
        Javalin app = Javalin.create().start(7050);
        List<Map<String, Object>> hubs = cleanCsv(/hubs-global.csv);

        app.get("/health", ctx -> ctx.result("OK"));
        app.get("/hubs"), ctx -> ctx.json(hubs));
    }
}
