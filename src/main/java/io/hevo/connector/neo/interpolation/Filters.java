package io.hevo.connector.neo.interpolation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hubspot.jinjava.interpret.JinjavaInterpreter;
import com.hubspot.jinjava.lib.filter.Filter;
import io.hevo.connector.neo.manifest.ManifestException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** Custom template filters, mirroring airbyte_cdk/sources/declarative/interpolation/filters.py. */
public final class Filters {

  private static final Map<String, String> HASH_ALGORITHMS =
      Map.of(
          "md5", "MD5",
          "sha1", "SHA-1",
          "sha224", "SHA-224",
          "sha256", "SHA-256",
          "sha384", "SHA-384",
          "sha512", "SHA-512");

  private Filters() {}

  public static List<Filter> all() {
    return List.of(
        new HashFilter(),
        new Base64EncodeFilter(),
        new Base64DecodeFilter(),
        new Base64BinasciiDecodeFilter(),
        new StringFilter(),
        new RegexSearchFilter(),
        new RegexReplaceFilter(),
        new HmacFilter());
  }

  private static String hexDigest(byte[] bytes) {
    StringBuilder sb = new StringBuilder(bytes.length * 2);
    for (byte b : bytes) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }

  static final class HashFilter implements Filter {
    @Override
    public String getName() {
      return "hash";
    }

    @Override
    public Object filter(Object var, JinjavaInterpreter interpreter, String... args) {
      String hashType = args.length > 0 && args[0] != null ? args[0] : "md5";
      String algorithm = HASH_ALGORITHMS.get(hashType);
      if (algorithm == null) {
        throw new ManifestException("Unsupported hash type: " + hashType);
      }
      try {
        MessageDigest digest = MessageDigest.getInstance(algorithm);
        digest.update(String.valueOf(var).getBytes(StandardCharsets.UTF_8));
        if (args.length > 1 && args[1] != null) {
          digest.update(String.valueOf(args[1]).getBytes(StandardCharsets.UTF_8));
        }
        return hexDigest(digest.digest());
      } catch (NoSuchAlgorithmException e) {
        throw new ManifestException("Hash algorithm unavailable: " + algorithm, e);
      }
    }
  }

  static final class Base64EncodeFilter implements Filter {
    @Override
    public String getName() {
      return "base64encode";
    }

    @Override
    public Object filter(Object var, JinjavaInterpreter interpreter, String... args) {
      return Base64.getEncoder()
          .encodeToString(String.valueOf(var).getBytes(StandardCharsets.UTF_8));
    }
  }

  static final class Base64DecodeFilter implements Filter {
    @Override
    public String getName() {
      return "base64decode";
    }

    @Override
    public Object filter(Object var, JinjavaInterpreter interpreter, String... args) {
      return new String(Base64.getDecoder().decode(String.valueOf(var)), StandardCharsets.UTF_8);
    }
  }

  /**
   * Faithful port of a CDK quirk: despite its name, base64binascii_decode ENCODES the value
   * (standard_b64encode of the ascii bytes).
   */
  static final class Base64BinasciiDecodeFilter implements Filter {
    @Override
    public String getName() {
      return "base64binascii_decode";
    }

    @Override
    public Object filter(Object var, JinjavaInterpreter interpreter, String... args) {
      return Base64.getEncoder()
          .encodeToString(String.valueOf(var).getBytes(StandardCharsets.US_ASCII));
    }
  }

  /** Strings pass through; other values become triple-quoted JSON, matching the CDK. */
  static final class StringFilter implements Filter {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public String getName() {
      return "string";
    }

    @Override
    public Object filter(Object var, JinjavaInterpreter interpreter, String... args) {
      if (var instanceof String) {
        return var;
      }
      try {
        return "\"\"\"" + MAPPER.writeValueAsString(var) + "\"\"\"";
      } catch (JsonProcessingException e) {
        throw new ManifestException("Cannot serialize value for |string filter", e);
      }
    }
  }

  /** First capture group of the first match, or the whole match if no groups; "" if no match. */
  static final class RegexSearchFilter implements Filter {
    @Override
    public String getName() {
      return "regex_search";
    }

    @Override
    public Object filter(Object var, JinjavaInterpreter interpreter, String... args) {
      if (args.length < 1 || args[0] == null) {
        throw new ManifestException("regex_search requires a regex argument");
      }
      Matcher matcher = Pattern.compile(args[0]).matcher(String.valueOf(var));
      if (matcher.find() && matcher.groupCount() >= 1) {
        return matcher.group(1);
      }
      return "";
    }
  }

  static final class RegexReplaceFilter implements Filter {
    @Override
    public String getName() {
      return "regex_replace";
    }

    @Override
    public Object filter(Object var, JinjavaInterpreter interpreter, String... args) {
      if (args.length < 2) {
        throw new ManifestException("regex_replace requires regex and replacement");
      }
      String replacement = args[1] == null ? "" : args[1];
      return Pattern.compile(args[0])
          .matcher(String.valueOf(var))
          .replaceAll(Matcher.quoteReplacement(replacement));
    }
  }

  /** HMAC hexdigest; the CDK only permits sha256. */
  static final class HmacFilter implements Filter {
    @Override
    public String getName() {
      return "hmac";
    }

    @Override
    public Object filter(Object var, JinjavaInterpreter interpreter, String... args) {
      if (args.length < 1 || args[0] == null) {
        throw new ManifestException("hmac requires a key argument");
      }
      String hashType = args.length > 1 && args[1] != null ? args[1] : "sha256";
      if (!"sha256".equals(hashType)) {
        throw new ManifestException("Unsupported hmac hash type: " + hashType);
      }
      try {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(args[0].getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        return hexDigest(mac.doFinal(String.valueOf(var).getBytes(StandardCharsets.UTF_8)));
      } catch (Exception e) {
        throw new ManifestException("Failed to compute hmac", e);
      }
    }
  }
}
