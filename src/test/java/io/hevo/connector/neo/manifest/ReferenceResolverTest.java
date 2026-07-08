package io.hevo.connector.neo.manifest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import org.junit.jupiter.api.Test;

/** Cases mirror the docstring examples in manifest_reference_resolver.py. */
class ReferenceResolverTest {

    private final ReferenceResolver resolver = new ReferenceResolver();

    @Test
    void resolvesBareStringReference() {
        Map<String, Object> manifest =
                ManifestLoader.load(
                        """
                        key: 1234
                        reference: "#/key"
                        """);
        Map<String, Object> resolved = resolver.preprocessManifest(manifest);
        assertEquals(1234, resolved.get("reference"));
    }

    @Test
    void refKeyMergesWithSiblingsAndSiblingsWin() {
        Map<String, Object> manifest =
                ManifestLoader.load(
                        """
                        key_value_pairs:
                          k1: v1
                          k2: v2
                        same:
                          $ref: "#/key_value_pairs"
                          k2: override
                          k3: v3
                        """);
        Map<String, Object> resolved = resolver.preprocessManifest(manifest);
        assertEquals(
                Map.of("k1", "v1", "k2", "override", "k3", "v3"), resolved.get("same"));
    }

    @Test
    void nestedPathPrefersLiteralKeyAtTopLevel() {
        Map<String, Object> manifest =
                ManifestLoader.load(
                        """
                        nested:
                          path: "first one"
                        nested/path: "uh oh"
                        value: "#/nested/path"
                        """);
        Map<String, Object> resolved = resolver.preprocessManifest(manifest);
        assertEquals("uh oh", resolved.get("value"));
    }

    @Test
    void nestedPathTraversesWhenNoLiteralKey() {
        Map<String, Object> manifest =
                ManifestLoader.load(
                        """
                        dict:
                          limit: 50
                        limit_ref: "#/dict/limit"
                        """);
        Map<String, Object> resolved = resolver.preprocessManifest(manifest);
        assertEquals(50, resolved.get("limit_ref"));
    }

    @Test
    void listIndexPathSegments() {
        Map<String, Object> manifest =
                ManifestLoader.load(
                        """
                        items:
                          - first
                          - second
                        pick: "#/items/1"
                        """);
        Map<String, Object> resolved = resolver.preprocessManifest(manifest);
        assertEquals("second", resolved.get("pick"));
    }

    @Test
    void circularReferenceThrows() {
        Map<String, Object> manifest =
                ManifestLoader.load(
                        """
                        a: "#/b"
                        b: "#/a"
                        """);
        assertThrows(
                ManifestException.CircularReference.class,
                () -> resolver.preprocessManifest(manifest));
    }

    @Test
    void undefinedReferenceThrows() {
        Map<String, Object> manifest = ManifestLoader.load("a: \"#/missing\"\n");
        assertThrows(
                ManifestException.UndefinedReference.class,
                () -> resolver.preprocessManifest(manifest));
    }
}
