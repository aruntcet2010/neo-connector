package io.hevo.connector;

import io.hevo.catalog.core.namespace.Namespace;
import io.hevo.connector.cdk.saas.SaasConnector;
import io.hevo.connector.exceptions.ConnectorException;
import io.hevo.connector.hooks.PlatformHooks;
import io.hevo.connector.model.ConnectorContext;
import io.hevo.connector.model.ConnectorInitContext;
import io.hevo.connector.model.ExecutionResult;
import io.hevo.connector.model.PollTaskNode;
import io.hevo.connector.processor.ConnectorProcessor;
import io.hevo.connector.neo.manifest.ManifestPipeline;
import io.hevo.datapath.common.config.enums.CategoryType;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic declarative connector: interprets a connector manifest (Airbyte low-code YAML) at
 * runtime and syncs the streams it defines.
 *
 * <p>P0 skeleton: loads and validates the manifest at init. Stream schema synthesis, task
 * generation, and the interpreter read loop land in the next phases.
 */
public class NeoConnector extends SaasConnector {

    private static final Logger log = LoggerFactory.getLogger(NeoConnector.class);

    private ConnectorInitContext initContext;
    private ManifestPipeline.Manifest manifest;

    @Override
    public void init(ConnectorInitContext context) {
        this.initContext = context;
        // P0: the manifest arrives inline for development. The manifest-store fetch keyed by
        // `manifest_ref` replaces this once the store exists.
        if (context.hasProperty("manifest_yaml")) {
            this.manifest = new ManifestPipeline().process(context.getString("manifest_yaml"));
            log.info(
                    "Loaded manifest version {} with {} streams",
                    manifest.version(),
                    manifest.streams().size());
        }
    }

    @Override
    public List<String> getSchemaFiles() {
        // Schemas are synthesized from the manifest at runtime; there is no static schema.yml.
        return List.of();
    }

    @Override
    public ExecutionResult fetchDataFromSource(
            ConnectorContext context, ConnectorProcessor processor) throws ConnectorException {
        throw new ConnectorException(
                "The fetchDataFromSource pattern is deprecated for this connector; it uses the"
                        + " CDK PollTask pattern via generateTasks().");
    }

    @Override
    public List<PollTaskNode> generateTasks(
            CategoryType categoryType, PlatformHooks platformHooks, Set<Namespace> activeObjects) {
        // P1: one ManifestPollTask per active stream resolved from the manifest.
        return List.of();
    }

    ManifestPipeline.Manifest manifest() {
        return manifest;
    }
}
