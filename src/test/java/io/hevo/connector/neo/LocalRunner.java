package io.hevo.connector.neo;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.hevo.connector.cdk.saas.http.SaasHttpClient;
import io.hevo.connector.neo.manifest.ManifestPipeline;
import io.hevo.connector.neo.runtime.ManifestPollTask;
import io.hevo.connector.neo.runtime.SchemaSynthesizer;
import io.hevo.connector.neo.runtime.StreamSpec;
import io.hevo.datapath.common.config.enums.CategoryType;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Dev tool: run any manifest against the REAL API from the command line.
 *
 * <pre>
 * ./gradlew runManifest --args="--manifest m.yaml"                          # validate + list streams
 * ./gradlew runManifest --args="--manifest m.yaml --config c.json --stream items --limit 10"
 * ./gradlew runManifest --args="--manifest m.yaml --config c.json --state '{\"updated_at\":\"...\"}'"
 * </pre>
 *
 * Prints records as JSON lines and the closing cursor state.
 */
public final class LocalRunner {

    private static final ObjectMapper JSON = new ObjectMapper();

    /** Thrown by the record consumer to stop a read after --limit records. */
    private static final class LimitReached extends RuntimeException {}

    public static void main(String[] args) throws Exception {
        Map<String, String> opts = parseArgs(args);
        String manifestPath = require(opts, "manifest");

        System.out.println("=== Loading manifest: " + manifestPath);
        ManifestPipeline.Manifest manifest =
                new ManifestPipeline().process(Files.readString(Path.of(manifestPath)));
        System.out.println("Manifest version: " + manifest.version());

        List<StreamSpec> specs = new ArrayList<>();
        for (Map<String, Object> stream : manifest.streams()) {
            try {
                StreamSpec spec = new StreamSpec(stream);
                specs.add(spec);
                System.out.printf(
                        "  stream: %-28s pk=%s incremental=%s substream=%s%n",
                        spec.name(),
                        spec.primaryKey(),
                        spec.incrementalSync() != null,
                        spec.partitionRouter() != null);
            } catch (RuntimeException e) {
                System.out.printf("  stream: UNSUPPORTED — %s%n", e.getMessage());
            }
        }
        System.out.println("=== Validation OK (" + specs.size() + " supported streams)");

        if (!opts.containsKey("config")) {
            System.out.println("No --config given; stopping after validation.");
            return;
        }

        Map<String, Object> config =
                JSON.readValue(
                        Files.readString(Path.of(opts.get("config"))),
                        new TypeReference<Map<String, Object>>() {});
        int limit = Integer.parseInt(opts.getOrDefault("limit", "10"));
        String only = opts.get("stream");
        String state = opts.get("state");

        try (SaasHttpClient client = SaasHttpClient.create()) {
            for (StreamSpec spec : specs) {
                if (only != null && !only.equals(spec.name())) {
                    continue;
                }
                System.out.println("\n=== Reading stream: " + spec.name() + " (limit " + limit + ")");
                try {
                    // Show the schema we'd declare to Hevo.
                    var schema = SchemaSynthesizer.synthesize(spec);
                    System.out.println("    fields: " + schema.fields().size());
                } catch (RuntimeException e) {
                    System.out.println("    schema synthesis failed: " + e.getMessage());
                }
                ManifestPollTask task =
                        new ManifestPollTask(null, CategoryType.HISTORICAL, spec, client, config);
                List<Map<String, Object>> records = new ArrayList<>();
                ManifestPollTask.ReadResult result;
                try {
                    result =
                            task.read(
                                    record -> {
                                        records.add(record);
                                        if (records.size() >= limit) {
                                            throw new LimitReached();
                                        }
                                    },
                                    state);
                } catch (LimitReached stopped) {
                    result = new ManifestPollTask.ReadResult(records.size(), null);
                    System.out.println("    (stopped at limit; state not closed)");
                } catch (RuntimeException e) {
                    System.out.println("    READ FAILED: " + e);
                    continue;
                }
                for (Map<String, Object> record : records) {
                    System.out.println(JSON.writeValueAsString(record));
                }
                System.out.println(
                        "    records=" + result.recordCount() + " state=" + result.stateJson());
            }
        }
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> opts = new LinkedHashMap<>();
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].startsWith("--")) {
                opts.put(args[i].substring(2), args[i + 1]);
            }
        }
        return opts;
    }

    private static String require(Map<String, String> opts, String key) {
        String value = opts.get(key);
        if (value == null) {
            throw new IllegalArgumentException("Missing required option --" + key);
        }
        return value;
    }
}
