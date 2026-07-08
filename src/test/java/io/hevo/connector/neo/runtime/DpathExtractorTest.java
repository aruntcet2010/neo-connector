package io.hevo.connector.neo.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.hevo.connector.neo.manifest.ManifestLoader;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DpathExtractorTest {

    private static DpathExtractor extractor(String yaml) {
        return new DpathExtractor(ManifestLoader.load(yaml), Map.of());
    }

    @Test
    void simpleFieldPath() {
        DpathExtractor e = extractor("type: DpathExtractor\nfield_path: [\"content\"]");
        List<Object> records = e.extractRecords("{\"content\": [{\"id\": 1}, {\"id\": 2}]}");
        assertEquals(2, records.size());
        assertEquals(Map.of("id", 1), records.get(0));
    }

    @Test
    void emptyPathYieldsWholeBody() {
        DpathExtractor e = extractor("type: DpathExtractor\nfield_path: []");
        // Root array → each element is a record
        assertEquals(2, e.extractRecords("[{\"a\": 1}, {\"a\": 2}]").size());
        // Root object → single record
        assertEquals(1, e.extractRecords("{\"a\": 1}").size());
    }

    @Test
    void nestedPathAndSingleObject() {
        DpathExtractor e =
                extractor("type: DpathExtractor\nfield_path: [\"data\", \"user\"]");
        List<Object> records = e.extractRecords("{\"data\": {\"user\": {\"id\": 7}}}");
        assertEquals(List.of(Map.of("id", 7)), records);
    }

    @Test
    void wildcardFansOut() {
        DpathExtractor e =
                extractor("type: DpathExtractor\nfield_path: [\"data\", \"*\", \"record\"]");
        List<Object> records =
                e.extractRecords(
                        "{\"data\": {\"a\": {\"record\": {\"id\": 1}}, \"b\": {\"record\":"
                                + " {\"id\": 2}}}}");
        assertEquals(2, records.size());
    }

    @Test
    void missingPathYieldsNothing() {
        DpathExtractor e = extractor("type: DpathExtractor\nfield_path: [\"missing\"]");
        assertTrue(e.extractRecords("{\"content\": []}").isEmpty());
    }
}
