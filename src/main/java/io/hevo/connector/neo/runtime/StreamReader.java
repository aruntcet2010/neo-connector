package io.hevo.connector.neo.runtime;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.hevo.connector.cdk.saas.http.SaasHttpClient;
import io.hevo.connector.cdk.saas.http.SaasHttpRequest;
import io.hevo.connector.cdk.saas.http.SaasHttpResponse;
import io.hevo.connector.neo.manifest.ManifestException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The manifest read engine: for each partition (router) × datetime window (cursor), paginate; per
 * page, extract → filter → transform → hand each record to the consumer; the cursor observes
 * records and its closing value becomes the returned state (legacy-CDK-shaped
 * {@code {cursor_field: value}} JSON).
 *
 * <p>This is the single execution path shared by the production poll task
 * ({@link ManifestPollTask}) and the test-read CLI — verification and production run the same
 * code by construction. {@link ReadLimits} bounds a read for cheap live verification;
 * {@link Evidence} observes every request/response for test-read reports.
 */
public final class StreamReader {

  private static final Logger log = LoggerFactory.getLogger(StreamReader.class);
  private static final ObjectMapper JSON = new ObjectMapper();

  /** Backstop against paginators that never terminate (misconfigured cursors). */
  private static final int DEFAULT_MAX_PAGES = 10_000;

  /** Bounds for a test read; null means unbounded. */
  public record ReadLimits(Integer maxRecords, Integer maxPagesPerSlice, Integer maxSlices) {
    public static ReadLimits unbounded() {
      return new ReadLimits(null, null, null);
    }
  }

  public record ReadResult(long recordCount, String stateJson, boolean limitReached) {}

  /** Observer for test-read evidence; production reads pass {@link #NONE}. */
  public interface Evidence {
    Evidence NONE = new Evidence() {};

    default void sliceStart(Map<String, Object> streamSlice) {}

    default void page(SaasHttpRequest request, SaasHttpResponse<String> response, int records) {}

    /** Called after each slice with the cursor state as of that point (null when no cursor). */
    default void sliceEnd(String stateJson) {}
  }

  private final StreamSpec stream;
  private final SaasHttpClient httpClient;
  private final Map<String, Object> config;
  private final NeoRequester requester;
  private final Paginator paginator;
  private final DpathExtractor extractor;
  private final RecordPipeline recordPipeline;
  private final PartitionRouters.Router partitionRouter;

  public StreamReader(StreamSpec stream, SaasHttpClient httpClient, Map<String, Object> config) {
    this.stream = stream;
    this.httpClient = httpClient;
    this.config = config;
    this.requester = new NeoRequester(stream.requester(), config);
    this.paginator = Paginator.from(stream.paginator(), config);
    this.extractor = new DpathExtractor(stream.extractor(), config);
    this.recordPipeline =
        new RecordPipeline(stream.recordSelector(), stream.transformations(), config);
    this.partitionRouter =
        PartitionRouters.from(stream.partitionRouter(), config, this::readParentStream);
  }

  public ReadResult read(Consumer<Map<String, Object>> recordConsumer, String stateJson) {
    return read(recordConsumer, stateJson, ReadLimits.unbounded(), Evidence.NONE);
  }

  public ReadResult read(
      Consumer<Map<String, Object>> recordConsumer,
      String stateJson,
      ReadLimits limits,
      Evidence evidence) {
    DatetimeCursor cursor =
        stream.incrementalSync() == null
            ? null
            : new DatetimeCursor(stream.incrementalSync(), config);
    if (cursor != null && stateJson != null) {
      Map<String, Object> state = parseState(stateJson);
      Object cursorValue = state.get(cursor.cursorField());
      if (cursorValue != null) {
        cursor.setInitialState(String.valueOf(cursorValue));
      }
    }

    long total = 0;
    int slicesRead = 0;
    boolean limitReached = false;

    outer:
    for (PartitionRouters.Partition partition : partitionRouter.partitions()) {
      List<Map<String, Object>> windows = cursor == null ? List.of(Map.of()) : cursor.windows();
      for (Map<String, Object> window : windows) {
        if (limits.maxSlices() != null && slicesRead >= limits.maxSlices()) {
          limitReached = true;
          break outer;
        }
        slicesRead++;

        Map<String, Object> streamSlice = new LinkedHashMap<>(window);
        streamSlice.putAll(partition.values());
        evidence.sliceStart(streamSlice);
        NeoRequester.SliceOptions slice =
            new NeoRequester.SliceOptions(
                streamSlice,
                cursor == null
                    ? List.of(partition.requestOptions())
                    : List.of(partition.requestOptions(), cursorOptions(cursor, window)));

        DatetimeCursor sliceCursor = cursor;
        SliceOutcome outcome =
            readSlice(
                slice,
                record -> {
                  Map<String, Object> processed = recordPipeline.process(record, streamSlice);
                  if (processed != null) {
                    if (sliceCursor != null) {
                      sliceCursor.observe(processed, window);
                    }
                    recordConsumer.accept(processed);
                  }
                },
                remainingRecords(limits, total),
                limits.maxPagesPerSlice(),
                evidence);
        total += outcome.records();
        evidence.sliceEnd(currentState(cursor));
        if (outcome.limitReached()) {
          limitReached = true;
        }
        if (limits.maxRecords() != null && total >= limits.maxRecords()) {
          limitReached = true;
          break outer;
        }
      }
    }
    return new ReadResult(total, currentState(cursor), limitReached);
  }

  private static Integer remainingRecords(ReadLimits limits, long alreadyRead) {
    if (limits.maxRecords() == null) {
      return null;
    }
    return (int) Math.max(0, limits.maxRecords() - alreadyRead);
  }

  private String currentState(DatetimeCursor cursor) {
    if (cursor == null || cursor.state() == null) {
      return null;
    }
    return writeState(Map.of(cursor.cursorField(), cursor.state()));
  }

  private static Map<RequestOptionSpec.InjectInto, Map<String, Object>> cursorOptions(
      DatetimeCursor cursor, Map<String, Object> window) {
    Map<RequestOptionSpec.InjectInto, Map<String, Object>> options = new LinkedHashMap<>();
    for (RequestOptionSpec.InjectInto target : RequestOptionSpec.InjectInto.values()) {
      Map<String, Object> targetOptions = cursor.requestOptions(target, window);
      if (!targetOptions.isEmpty()) {
        options.put(target, targetOptions);
      }
    }
    return options;
  }

  private record SliceOutcome(long records, boolean limitReached) {}

  /** Paginates one slice, invoking the consumer per raw extracted record. */
  private SliceOutcome readSlice(
      NeoRequester.SliceOptions slice,
      Consumer<Map<String, Object>> recordConsumer,
      Integer maxRecords,
      Integer maxPages,
      Evidence evidence) {
    long total = 0;
    int pageCap = maxPages != null ? maxPages : DEFAULT_MAX_PAGES;
    Object token = paginator.initialToken();
    for (int page = 0; page < pageCap; page++) {
      SaasHttpRequest request = requester.buildRequest(stream.name(), token, paginator, slice);
      SaasHttpResponse<String> response = httpClient.execute(request);
      List<Object> records =
          response.isSuccess() ? extractor.extractRecords(response.body()) : List.of();
      evidence.page(request, response, records.size());
      response.throwForStatus();

      Object lastRecord = records.isEmpty() ? null : records.get(records.size() - 1);
      for (Object record : records) {
        recordConsumer.accept(asMap(record));
      }
      total += records.size();

      Paginator.PageContext context =
          new Paginator.PageContext(
              DpathExtractor.decode(response.body()),
              Paginator.flattenHeaders(response.headers()),
              records.size(),
              lastRecord,
              token);
      token = paginator.nextPageToken(context);
      if (token == null) {
        return new SliceOutcome(total, false);
      }
      if (maxRecords != null && total >= maxRecords) {
        return new SliceOutcome(total, true);
      }
    }
    if (maxPages == null) {
      log.warn(
          "Stream {}: pagination did not terminate after {} pages", stream.name(), pageCap);
    }
    return new SliceOutcome(total, maxPages != null);
  }

  /** Reads a parent DeclarativeStream (for SubstreamPartitionRouter), full refresh. */
  private List<Object> readParentStream(Map<String, Object> parentStreamComponent) {
    StreamSpec parentSpec = new StreamSpec(parentStreamComponent);
    if (parentSpec.partitionRouter() != null) {
      throw new ManifestException(
          "Nested partition routers (parent stream "
              + parentSpec.name()
              + " has its own router) are not supported");
    }
    StreamReader parentReader = new StreamReader(parentSpec, httpClient, config);
    List<Object> records = new ArrayList<>();
    parentReader.read(records::add, null);
    return records;
  }

  private static Map<String, Object> parseState(String stateJson) {
    try {
      return JSON.readValue(stateJson, new TypeReference<Map<String, Object>>() {});
    } catch (Exception e) {
      log.warn("Ignoring unparseable stream state: {}", stateJson);
      return Map.of();
    }
  }

  private static String writeState(Map<String, Object> state) {
    try {
      return JSON.writeValueAsString(state);
    } catch (Exception e) {
      throw new ManifestException("Failed to serialize stream state", e);
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> asMap(Object record) {
    if (record instanceof Map<?, ?> map) {
      return (Map<String, Object>) map;
    }
    Map<String, Object> wrapped = new LinkedHashMap<>();
    wrapped.put("value", record);
    return wrapped;
  }
}
