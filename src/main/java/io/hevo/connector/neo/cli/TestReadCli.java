package io.hevo.connector.neo.cli;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.hevo.connector.cdk.saas.http.SaasHttpClient;
import io.hevo.connector.cdk.saas.http.SaasHttpRequest;
import io.hevo.connector.cdk.saas.http.SaasHttpResponse;
import io.hevo.connector.neo.manifest.ManifestPipeline;
import io.hevo.connector.neo.runtime.Components;
import io.hevo.connector.neo.runtime.StreamReader;
import io.hevo.connector.neo.runtime.StreamSpec;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The harness-facing engine CLI. Two subcommands, JSON report on stdout, logs on stderr:
 *
 * <pre>
 * java -jar neo-testread.jar validate --manifest fragment.yaml
 * java -jar neo-testread.jar read --manifest fragment.yaml --stream orders \
 *     --config-file credentials.json [--state '{"cursor":"..."}' | --state-file s.json] \
 *     [--page-size 2] [--max-records 20] [--max-pages 2] [--max-slices 3] [--no-validate] \
 *     [--secret-keys api_key,token]
 * </pre>
 *
 * `validate` runs the manifest pipeline plus per-stream support checks (StreamSpec construction)
 * — gate 1 (valid DSL) and gate 2 (this engine implements it) — with no HTTP.
 *
 * <p>`read` executes one stream against the real API with bounded limits and reports per-page
 * request/response evidence (secret values masked), a records sample, and the emitted cursor
 * state (re-injectable via --state). The report shape mirrors the harness's Python
 * TestReadReport so both engines' reports are comparable.
 */
public final class TestReadCli {

  private static final ObjectMapper JSON = new ObjectMapper();
  private static final int MAX_BODY_CHARS = 2000;
  private static final int MAX_RECORD_SAMPLE = 10;
  private static final StreamReader.ReadLimits DEFAULT_LIMITS =
      new StreamReader.ReadLimits(20, 2, 3);

  private TestReadCli() {}

  public static void main(String[] args) {
    System.exit(run(args, System.out));
  }

  static int run(String[] args, PrintStream out) {
    try {
      if (args.length == 0) {
        throw new IllegalArgumentException("Usage: validate|read --manifest <path> ...");
      }
      String command = args[0];
      Map<String, String> opts = parseOpts(args);
      Map<String, Object> report =
          switch (command) {
            case "validate" -> validate(opts);
            case "read" -> read(opts);
            default -> throw new IllegalArgumentException("Unknown command: " + command);
          };
      out.println(JSON.writeValueAsString(report));
      return 0;
    } catch (Exception e) {
      try {
        out.println(
            JSON.writeValueAsString(
                Map.of("success", false, "errors", List.of(errorEntry(e)))));
      } catch (Exception ignored) {
        // last resort below
      }
      return 1;
    }
  }

  // ------------------------------------------------------------------ validate

  private static Map<String, Object> validate(Map<String, String> opts) throws Exception {
    Path manifestPath = Path.of(required(opts, "manifest"));
    Map<String, Object> report = new LinkedHashMap<>();
    ManifestPipeline.Manifest manifest;
    try {
      manifest = new ManifestPipeline().process(Files.readString(manifestPath));
    } catch (Exception e) {
      report.put("valid", false);
      report.put("errors", List.of(errorEntry(e)));
      return report;
    }
    List<Map<String, Object>> streams = new ArrayList<>();
    boolean allSupported = true;
    for (Map<String, Object> streamComponent : manifest.streams()) {
      Map<String, Object> entry = new LinkedHashMap<>();
      entry.put("name", Components.getString(streamComponent, "name"));
      try {
        StreamSpec spec = new StreamSpec(streamComponent);
        // Constructing the full runtime graph (requester incl. authenticator, paginator,
        // extractor, transformations, partition router) is the real gate 2: a component the
        // engine cannot execute throws here, not just at read time. No HTTP happens —
        // construction is lazy about evaluation.
        new StreamReader(spec, null, Map.of());
        entry.put("name", spec.name());
        entry.put("supported", true);
        entry.put("primary_key", spec.primaryKey());
        entry.put("incremental", spec.incrementalSync() != null);
        entry.put("substream", spec.partitionRouter() != null);
      } catch (RuntimeException e) {
        allSupported = false;
        entry.put("supported", false);
        entry.put("reason", e.getMessage());
      }
      streams.add(entry);
    }
    report.put("valid", true);
    report.put("all_streams_supported", allSupported);
    report.put("version", manifest.version());
    report.put("streams", streams);
    return report;
  }

  // ---------------------------------------------------------------------- read

  private static Map<String, Object> read(Map<String, String> opts) throws Exception {
    Path manifestPath = Path.of(required(opts, "manifest"));
    String streamName = required(opts, "stream");
    Map<String, Object> config =
        JSON.readValue(
            Files.readString(Path.of(required(opts, "config-file"))),
            new TypeReference<Map<String, Object>>() {});

    String state = opts.get("state");
    if (state == null && opts.containsKey("state-file")) {
      state = Files.readString(Path.of(opts.get("state-file")));
    }
    StreamReader.ReadLimits limits =
        new StreamReader.ReadLimits(
            intOpt(opts, "max-records", DEFAULT_LIMITS.maxRecords()),
            intOpt(opts, "max-pages", DEFAULT_LIMITS.maxPagesPerSlice()),
            intOpt(opts, "max-slices", DEFAULT_LIMITS.maxSlices()));

    // Over-redact by default: every config value is treated as a secret.
    Set<String> secretKeys =
        opts.containsKey("secret-keys")
            ? Set.of(opts.get("secret-keys").split(","))
            : config.keySet();
    Set<String> secretValues =
        secretKeys.stream()
            .map(config::get)
            .filter(v -> v != null && !String.valueOf(v).isEmpty())
            .map(String::valueOf)
            .collect(Collectors.toSet());

    boolean validate = !opts.containsKey("no-validate");
    ManifestPipeline.Manifest manifest =
        new ManifestPipeline(validate).process(Files.readString(manifestPath));
    Map<String, Object> streamComponent = manifest.stream(streamName);
    if (streamComponent == null) {
      throw new IllegalArgumentException(
          "Stream "
              + streamName
              + " not in manifest (has "
              + manifest.streams().stream().map(s -> Components.getString(s, "name")).toList()
              + ")");
    }

    String pageSizeOverride = null;
    if (opts.containsKey("page-size")) {
      pageSizeOverride =
          applyPageSizeOverride(streamComponent, Integer.parseInt(opts.get("page-size")));
    }

    EvidenceReport evidence = new EvidenceReport(secretValues);
    List<Map<String, Object>> sample = new ArrayList<>();
    long[] count = {0};
    StreamReader.ReadResult result = null;
    Exception failure = null;
    try (SaasHttpClient client = SaasHttpClient.create()) {
      StreamReader reader = new StreamReader(new StreamSpec(streamComponent), client, config);
      result =
          reader.read(
              record -> {
                count[0]++;
                if (sample.size() < MAX_RECORD_SAMPLE) {
                  sample.add(record);
                }
              },
              state,
              limits,
              evidence);
    } catch (Exception e) {
      failure = e;
    }

    boolean anyPage =
        evidence.slices.stream().anyMatch(s -> !((List<?>) s.get("pages")).isEmpty());
    Map<String, Object> report = new LinkedHashMap<>();
    report.put("stream", streamName);
    report.put("success", failure == null && anyPage);
    report.put("slices", evidence.slices);
    report.put(
        "errors", failure == null ? List.of() : List.of(evidence.redactEntry(errorEntry(failure))));
    report.put("state_after", result == null ? parseStateOrNull(null) : parseStateOrNull(result.stateJson()));
    report.put("records_sample", sample);
    report.put("record_count", count[0]);
    report.put("limit_reached", result != null && result.limitReached());
    report.put("page_size_override", pageSizeOverride);
    return report;
  }

  /** Mirrors the harness's Python apply_page_size_override: mutate the pagination strategy. */
  @SuppressWarnings("unchecked")
  static String applyPageSizeOverride(Map<String, Object> streamComponent, int pageSize) {
    Map<String, Object> retriever = Components.getMap(streamComponent, "retriever");
    Map<String, Object> paginator = Components.getMap(retriever, "paginator");
    if (paginator == null || !"DefaultPaginator".equals(Components.type(paginator))) {
      return "no_paginator";
    }
    Map<String, Object> strategy = Components.getMap(paginator, "pagination_strategy");
    if (strategy == null) {
      return "no_paginator";
    }
    strategy.put("page_size", pageSize);
    return paginator.get("page_size_option") != null ? "applied" : "not_injectable";
  }

  /** Collects per-slice/per-page evidence with secret values masked. */
  private static final class EvidenceReport implements StreamReader.Evidence {
    private final Set<String> secretValues;
    final List<Map<String, Object>> slices = new ArrayList<>();
    private Map<String, Object> currentSlice;

    EvidenceReport(Set<String> secretValues) {
      this.secretValues = secretValues;
    }

    @Override
    public void sliceStart(Map<String, Object> streamSlice) {
      currentSlice = new LinkedHashMap<>();
      currentSlice.put("slice_descriptor", streamSlice.isEmpty() ? null : redactValue(streamSlice));
      currentSlice.put("state", null);
      currentSlice.put("pages", new ArrayList<Map<String, Object>>());
      slices.add(currentSlice);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void page(SaasHttpRequest request, SaasHttpResponse<String> response, int records) {
      Map<String, Object> page = new LinkedHashMap<>();
      Map<String, Object> req = new LinkedHashMap<>();
      req.put("url", redact(request.fullUrl()));
      req.put("http_method", request.method());
      req.put(
          "headers",
          request.headers().entrySet().stream()
              .collect(
                  Collectors.toMap(Map.Entry::getKey, e -> redact(e.getValue()), (a, b) -> a)));
      req.put(
          "body",
          request.body() == null ? null : redact(new String(request.body())));
      Map<String, Object> resp = new LinkedHashMap<>();
      resp.put("status", response.statusCode());
      String body = response.body() == null ? "" : redact(response.body());
      resp.put("body", body.length() > MAX_BODY_CHARS ? body.substring(0, MAX_BODY_CHARS) : body);
      page.put("request", req);
      page.put("response", resp);
      page.put("record_count", records);
      ((List<Map<String, Object>>) currentSlice.get("pages")).add(page);
    }

    @Override
    public void sliceEnd(String stateJson) {
      if (currentSlice != null && stateJson != null) {
        currentSlice.put("state", List.of(parseStateOrNull(stateJson)));
      }
    }

    private String redact(String value) {
      if (value == null) {
        return null;
      }
      String redacted = value;
      for (String secret : secretValues) {
        redacted = redacted.replace(secret, "****");
      }
      return redacted;
    }

    @SuppressWarnings("unchecked")
    private Object redactValue(Object value) {
      if (value instanceof String s) {
        return redact(s);
      }
      if (value instanceof Map<?, ?> map) {
        Map<String, Object> out = new LinkedHashMap<>();
        map.forEach((k, v) -> out.put(String.valueOf(k), redactValue(v)));
        return out;
      }
      if (value instanceof List<?> list) {
        return list.stream().map(EvidenceReport.this::redactValue).toList();
      }
      return value;
    }

    Map<String, Object> redactEntry(Map<String, Object> entry) {
      Map<String, Object> out = new LinkedHashMap<>();
      entry.forEach((k, v) -> out.put(k, v instanceof String s ? redact(s) : v));
      return out;
    }
  }

  // ------------------------------------------------------------------- helpers

  private static Map<String, Object> parseStateOrNull(String stateJson) {
    if (stateJson == null) {
      return null;
    }
    try {
      return JSON.readValue(stateJson, new TypeReference<Map<String, Object>>() {});
    } catch (Exception e) {
      return null;
    }
  }

  private static Map<String, Object> errorEntry(Exception e) {
    StringWriter stack = new StringWriter();
    e.printStackTrace(new PrintWriter(stack));
    Map<String, Object> entry = new LinkedHashMap<>();
    entry.put("message", String.valueOf(e.getMessage()));
    entry.put("internal_message", e.getClass().getName());
    entry.put("stacktrace", stack.toString());
    return entry;
  }

  private static Map<String, String> parseOpts(String[] args) {
    Map<String, String> opts = new LinkedHashMap<>();
    for (int i = 1; i < args.length; i++) {
      if (!args[i].startsWith("--")) {
        continue;
      }
      String key = args[i].substring(2);
      if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
        opts.put(key, args[++i]);
      } else {
        opts.put(key, "true"); // boolean flag, e.g. --no-validate
      }
    }
    return opts;
  }

  private static String required(Map<String, String> opts, String key) {
    String value = opts.get(key);
    if (value == null) {
      throw new IllegalArgumentException("Missing required option --" + key);
    }
    return value;
  }

  private static Integer intOpt(Map<String, String> opts, String key, Integer fallback) {
    return opts.containsKey(key) ? Integer.valueOf(opts.get(key)) : fallback;
  }
}
