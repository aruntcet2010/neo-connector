package io.hevo.connector.neo.manifest;

/** Raised when a manifest cannot be loaded, resolved, or validated. */
public class ManifestException extends RuntimeException {

  public ManifestException(String message) {
    super(message);
  }

  public ManifestException(String message, Throwable cause) {
    super(message, cause);
  }

  /** A `$ref`/"#/" reference chain points back to itself. */
  public static class CircularReference extends ManifestException {
    public CircularReference(String ref) {
      super("Circular reference detected: " + ref);
    }
  }

  /** A `$ref`/"#/" reference points to a path that does not exist in the manifest. */
  public static class UndefinedReference extends ManifestException {
    public UndefinedReference(String path, String ref) {
      super("Undefined reference " + ref + " (path: " + path + ")");
    }
  }
}
