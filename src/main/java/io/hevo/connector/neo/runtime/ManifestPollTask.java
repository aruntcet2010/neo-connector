package io.hevo.connector.neo.runtime;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.hevo.connector.cdk.model.PollResult;
import io.hevo.connector.cdk.offset.SimpleOffset;
import io.hevo.connector.cdk.saas.http.SaasHttpClient;
import io.hevo.connector.cdk.saas.http.SaasHttpRequest;
import io.hevo.connector.cdk.saas.http.SaasHttpResponse;
import io.hevo.connector.cdk.saas.task.SaasObjectPollTask;
import io.hevo.connector.hooks.PlatformHooks;
import io.hevo.connector.neo.manifest.ManifestException;
import io.hevo.datapath.common.config.enums.CategoryType;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The generic poll task interpreting one manifest stream. The read loop mirrors the CDK's
 * SimpleRetriever: for each partition (router) × datetime window (cursor), paginate; per page,
 * extract → filter → transform → publish; the cursor observes records and its closing value is
 * checkpointed into the {@link SimpleOffset} under {@link #STATE_KEY} as legacy-CDK-shaped JSON
 * ({cursor_field: value}).
 */
public class ManifestPollTask extends SaasObjectPollTask {

    private static final Logger log = LoggerFactory.getLogger(ManifestPollTask.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    static final String STATE_KEY = "state";
    /** Backstop against paginators that never terminate (misconfigured cursors). */
    private static final int MAX_PAGES = 10_000;

    private final StreamSpec stream;
    private final SaasHttpClient httpClient;
    private final Map<String, Object> config;
    private final NeoRequester requester;
    private final Paginator paginator;
    private final DpathExtractor extractor;
    private final RecordPipeline recordPipeline;
    private final PartitionRouters.Router partitionRouter;

    public ManifestPollTask(
            PlatformHooks platformHooks,
            CategoryType categoryType,
            StreamSpec stream,
            SaasHttpClient httpClient,
            Map<String, Object> config) {
        super(platformHooks, categoryType, stream.namespace());
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

    @Override
    public PollResult executePoll(SimpleOffset offset, CategoryType categoryType) {
        String stateJson =
                offset == null || offset.isEmpty() ? null : offset.get(STATE_KEY);
        ReadResult result =
                read(record -> publishRecord(stream.namespace(), record), stateJson);
        log.info("Stream {}: published {} records", stream.name(), result.recordCount());
        SimpleOffset nextOffset = SimpleOffset.empty();
        if (result.stateJson() != null) {
            nextOffset.set(STATE_KEY, result.stateJson());
        }
        return PollResult.success(nextOffset);
    }

    public record ReadResult(long recordCount, String stateJson) {}

    /** Test-friendly entry: read everything with no prior state. */
    public long readStream(Consumer<Object> recordConsumer) {
        return read(r -> recordConsumer.accept(r), null).recordCount();
    }

    public ReadResult read(Consumer<Map<String, Object>> recordConsumer, String stateJson) {
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
        for (PartitionRouters.Partition partition : partitionRouter.partitions()) {
            List<Map<String, Object>> windows =
                    cursor == null ? List.of(Map.of()) : cursor.windows();
            for (Map<String, Object> window : windows) {
                Map<String, Object> streamSlice = new LinkedHashMap<>(window);
                streamSlice.putAll(partition.values());
                NeoRequester.SliceOptions slice =
                        new NeoRequester.SliceOptions(
                                streamSlice,
                                cursor == null
                                        ? List.of(partition.requestOptions())
                                        : List.of(
                                                partition.requestOptions(),
                                                cursorOptions(cursor, window)));
                total +=
                        readSlice(
                                slice,
                                record -> {
                                    Map<String, Object> processed =
                                            recordPipeline.process(record, streamSlice);
                                    if (processed != null) {
                                        if (cursor != null) {
                                            cursor.observe(processed, window);
                                        }
                                        recordConsumer.accept(processed);
                                    }
                                });
            }
        }
        String newState = null;
        if (cursor != null && cursor.state() != null) {
            newState = writeState(Map.of(cursor.cursorField(), cursor.state()));
        }
        return new ReadResult(total, newState);
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

    /** Paginates one slice, invoking the consumer per raw extracted record. */
    private long readSlice(
            NeoRequester.SliceOptions slice, Consumer<Map<String, Object>> recordConsumer) {
        long total = 0;
        Object token = paginator.initialToken();
        for (int page = 0; page < MAX_PAGES; page++) {
            SaasHttpRequest request =
                    requester.buildRequest(stream.name(), token, paginator, slice);
            SaasHttpResponse<String> response = httpClient.execute(request);
            response.throwForStatus();

            List<Object> records = extractor.extractRecords(response.body());
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
                return total;
            }
        }
        log.warn(
                "Stream {}: pagination did not terminate after {} pages", stream.name(), MAX_PAGES);
        return total;
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
        ManifestPollTask parentTask =
                new ManifestPollTask(
                        getPlatformHooks(), getCategoryType(), parentSpec, httpClient, config);
        List<Object> records = new java.util.ArrayList<>();
        parentTask.readStream(records::add);
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
