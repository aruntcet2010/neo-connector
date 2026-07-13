package io.hevo.connector.neo.manifest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Map;
import org.junit.jupiter.api.Test;

class ComponentTransformerTest {

  private final ComponentTransformer transformer = new ComponentTransformer();

  @Test
  @SuppressWarnings("unchecked")
  void infersDefaultTypesFromParentFieldIdentifier() {
    Map<String, Object> stream =
        ManifestLoader.load(
            """
                        type: DeclarativeStream
                        retriever:
                          requester:
                            url_base: "https://api.example.com"
                          record_selector:
                            extractor:
                              field_path: ["data"]
                        """);
    Map<String, Object> result = transformer.propagateTypesAndParameters("", stream, Map.of());
    Map<String, Object> retriever = (Map<String, Object>) result.get("retriever");
    assertEquals("SimpleRetriever", retriever.get("type"));
    assertEquals("HttpRequester", ((Map<String, Object>) retriever.get("requester")).get("type"));
    Map<String, Object> selector = (Map<String, Object>) retriever.get("record_selector");
    assertEquals("RecordSelector", selector.get("type"));
    assertEquals("DpathExtractor", ((Map<String, Object>) selector.get("extractor")).get("type"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void parametersPropagateToDescendantsAndCurrentLevelWins() {
    Map<String, Object> stream =
        ManifestLoader.load(
            """
                        type: DeclarativeStream
                        $parameters:
                          name: "from_parent"
                          shared: "parent_value"
                        retriever:
                          type: SimpleRetriever
                          $parameters:
                            shared: "child_value"
                          requester:
                            type: HttpRequester
                            url_base: "https://api.example.com"
                        """);
    Map<String, Object> result = transformer.propagateTypesAndParameters("", stream, Map.of());
    // Parameters materialize as fields on the owning component.
    assertEquals("from_parent", result.get("name"));
    Map<String, Object> retriever = (Map<String, Object>) result.get("retriever");
    assertEquals("child_value", retriever.get("shared"));
    // And keep flowing to descendants.
    Map<String, Object> requester = (Map<String, Object>) retriever.get("requester");
    assertEquals("child_value", requester.get("shared"));
    assertEquals("from_parent", requester.get("name"));
  }

  @Test
  void existingFieldsTakePrecedenceOverParameters() {
    Map<String, Object> component =
        ManifestLoader.load(
            """
                        type: DeclarativeStream
                        name: "explicit"
                        $parameters:
                          name: "from_parameters"
                        """);
    Map<String, Object> result = transformer.propagateTypesAndParameters("", component, Map.of());
    assertEquals("explicit", result.get("name"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void jsonSchemaObjectsAreLeftUntouched() {
    Map<String, Object> component =
        ManifestLoader.load(
            """
                        type: DeclarativeStream
                        $parameters:
                          name: "s"
                        schema_loader:
                          type: InlineSchemaLoader
                          schema:
                            type: object
                            properties:
                              id:
                                type: string
                        """);
    Map<String, Object> result = transformer.propagateTypesAndParameters("", component, Map.of());
    Map<String, Object> schema =
        (Map<String, Object>) ((Map<String, Object>) result.get("schema_loader")).get("schema");
    assertFalse(schema.containsKey("name"), "parameters must not leak into json schemas");
    Map<String, Object> idProperty =
        (Map<String, Object>) ((Map<String, Object>) schema.get("properties")).get("id");
    assertFalse(idProperty.containsKey("$parameters"));
  }
}
