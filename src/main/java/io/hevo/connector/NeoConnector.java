package io.hevo.connector;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.hevo.catalog.core.namespace.Namespace;
import io.hevo.connector.cdk.saas.SaasConnector;
import io.hevo.connector.exceptions.ConnectorException;
import io.hevo.connector.hooks.PlatformHooks;
import io.hevo.connector.model.ConnectorContext;
import io.hevo.connector.model.ConnectorInitContext;
import io.hevo.connector.model.ExecutionResult;
import io.hevo.connector.model.ObjectDetails;
import io.hevo.connector.model.ObjectFailureInfo;
import io.hevo.connector.model.ObjectSchema;
import io.hevo.connector.model.PollTaskNode;
import io.hevo.connector.neo.manifest.ManifestPipeline;
import io.hevo.connector.neo.runtime.Components;
import io.hevo.connector.neo.runtime.DpathExtractor;
import io.hevo.connector.neo.runtime.ManifestPollTask;
import io.hevo.connector.neo.runtime.NeoRequester;
import io.hevo.connector.neo.runtime.Paginator;
import io.hevo.connector.neo.runtime.SchemaSynthesizer;
import io.hevo.connector.neo.runtime.StreamSpec;
import io.hevo.connector.processor.ConnectorProcessor;
import io.hevo.datapath.common.config.enums.CategoryType;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic declarative connector: interprets a connector manifest (Airbyte low-code YAML) at runtime
 * and syncs the streams it defines.
 *
 * <p>The manifest arrives via the {@code manifest_yaml} config property (inline, for development; a
 * manifest-store fetch keyed by {@code manifest_ref} will replace this). The source settings the
 * manifest's spec declares arrive as a JSON object in {@code source_config_json}.
 */
public class NeoConnector extends SaasConnector {

  private static final Logger log = LoggerFactory.getLogger(NeoConnector.class);
  private static final ObjectMapper JSON = new ObjectMapper();

  private ManifestPipeline.Manifest manifest;
  private Map<String, Object> sourceConfig = Map.of();
  private final Map<String, StreamSpec> streamSpecs = new LinkedHashMap<>();

  @Override
  public void init(ConnectorInitContext context) {
    if (context.hasProperty("source_config_json")) {
      try {
        this.sourceConfig =
            JSON.readValue(
                context.getString("source_config_json"),
                new TypeReference<Map<String, Object>>() {});
      } catch (Exception e) {
        throw new ConnectorException("source_config_json is not valid JSON", e);
      }
    }
    if (context.hasProperty("manifest_yaml")) {
      loadManifest(context.getString("manifest_yaml"));
    }
  }

  private void loadManifest(String manifestYaml) {
    this.manifest = new ManifestPipeline().process(manifestYaml);
    streamSpecs.clear();
    for (Map<String, Object> stream : manifest.streams()) {
      StreamSpec spec = new StreamSpec(stream);
      streamSpecs.put(spec.name(), spec);
    }
    log.info("Loaded manifest version {} with {} streams", manifest.version(), streamSpecs.size());
  }

  @Override
  public List<String> getSchemaFiles() {
    // Schemas are synthesized from the manifest at runtime; there is no static schema.yml.
    return List.of();
  }

  @Override
  public void initializeConnection() throws ConnectorException {
    super.initializeConnection();
    if (manifest != null) {
      runCheckStream();
    }
  }

  /** The manifest's CheckStream: read the first page of the declared check stream. */
  private void runCheckStream() throws ConnectorException {
    Map<String, Object> check = manifest.check();
    List<Object> streamNames = check == null ? null : Components.getList(check, "stream_names");
    if (streamNames == null || streamNames.isEmpty()) {
      return;
    }
    String streamName = String.valueOf(streamNames.get(0));
    StreamSpec stream = streamSpecs.get(streamName);
    if (stream == null) {
      throw new ConnectorException("Check stream not found in manifest: " + streamName);
    }
    try {
      NeoRequester requester = new NeoRequester(stream.requester(), sourceConfig);
      Paginator paginator = Paginator.from(stream.paginator(), sourceConfig);
      var response =
          saasHttpClient.execute(
              requester.buildRequest(stream.name(), paginator.initialToken(), paginator));
      response.throwForStatus();
      new DpathExtractor(stream.extractor(), sourceConfig).extractRecords(response.body());
      log.info("Check stream {} succeeded", streamName);
    } catch (ConnectorException e) {
      throw e;
    } catch (Exception e) {
      throw new ConnectorException("Connection check failed on stream " + streamName, e);
    }
  }

  @Override
  public List<ObjectDetails> getObjects() throws ConnectorException {
    List<ObjectDetails> objects = new ArrayList<>();
    for (StreamSpec stream : streamSpecs.values()) {
      objects.add(SchemaSynthesizer.synthesize(stream).objectDetail());
    }
    return objects;
  }

  @Override
  public List<ObjectSchema> fetchSchemaFromSource(List<ObjectDetails> objects)
      throws ConnectorException {
    List<ObjectSchema> schemas = new ArrayList<>();
    for (ObjectDetails object : objects) {
      StreamSpec stream = streamSpecs.get(object.getTableFullyQualifiedName());
      if (stream != null) {
        schemas.add(SchemaSynthesizer.synthesize(stream));
      }
    }
    return schemas;
  }

  @Override
  public ExecutionResult fetchDataFromSource(ConnectorContext context, ConnectorProcessor processor)
      throws ConnectorException {
    throw new ConnectorException(
        "The fetchDataFromSource pattern is deprecated for this connector; it uses the"
            + " CDK PollTask pattern via generateTasks().");
  }

  @Override
  public List<PollTaskNode> generateTasks(
      CategoryType categoryType, PlatformHooks platformHooks, Set<Namespace> activeObjects) {
    List<PollTaskNode> tasks = new ArrayList<>();
    for (Namespace object : activeObjects) {
      StreamSpec stream = streamSpecs.get(object.k0());
      if (stream == null) {
        log.error("[ALERT] No manifest stream for object: {}", object.k0());
        platformHooks.markSourceObjectAsFailed(
            object,
            new ObjectFailureInfo.Builder()
                .setReason("No manifest stream named: " + object.k0())
                .setPermanent(true)
                .build());
        continue;
      }
      tasks.add(
          new PollTaskNode(
              stream.name(),
              new ManifestPollTask(
                  platformHooks, categoryType, stream, saasHttpClient, sourceConfig),
              List.of()));
    }
    return tasks;
  }

  ManifestPipeline.Manifest manifest() {
    return manifest;
  }
}
