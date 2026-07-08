# neo-connector

A generic Hevo connector that interprets a declarative connector manifest (Airbyte low-code
`manifest.yaml`) at runtime and syncs the streams it defines — one connector, no per-source Java.

It follows the standard Hevo connector contract (`SaasConnector`, framework V2, ServiceLoader
SPI) exactly like `ordergroove-connector` / `bamboohr-connector`; the difference is that the
per-stream poll-task classes are replaced by a manifest interpreter ported from the
[airbyte-python-cdk](https://github.com/airbytehq/airbyte-python-cdk) declarative framework.

## Status

| Phase | Scope | Status |
|---|---|---|
| P0 | Manifest pipeline (parse → `$ref` → `$parameters` → schema validation) + Jinja expression engine (macros, filters, Python-faithful coercion) | ✅ done, tested |
| P1 | Full-refresh read: HttpRequester, ApiKey/Bearer/Basic auth, DpathExtractor, paginators, schema synthesis → `ObjectSchema`, `CheckStream` | ⬜ |
| P2 | Incremental + partitions: DatetimeBasedCursor windowing, offset round-trip, List/Substream partition routers | ⬜ |
| P3 | OAuth, error-handler/backoff fidelity, transformations, manifest-store fetch, differential test harness vs the Python CDK | ⬜ |

Out of scope by design: custom Python components (`class_name` → `components.py`), AsyncRetriever,
dynamic streams. Manifests using those fall back to natively built connectors.

## Layout

```
src/main/java/io/hevo/connector/
├── NeoConnector.java                  # SaasConnector entry point (SPI-registered)
└── neo/
    ├── manifest/                      # static side: YAML → validated component tree
    │   ├── ManifestLoader             # YAML → Map (Jackson)
    │   ├── ReferenceResolver          # $ref / "#/path" resolution (sibling keys win)
    │   ├── ComponentTransformer       # $parameters propagation + default type inference
    │   ├── ManifestValidator          # networknt draft-07 validation against the bundled schema
    │   └── ManifestPipeline           # orchestration + typed Manifest accessor
    └── interpolation/                 # runtime side: Jinja expression engine
        ├── JinjaEngine                # jinjava + aliases + literal_eval coercion
        ├── InterpolatedString         # plain-string fast path + default fallback
        ├── Macros                     # now_utc, format_datetime, duration, timestamp, ...
        ├── Filters                    # hash, base64*, regex_*, string, hmac
        ├── PyFormat                   # strftime/strptime + %s/%ms/%s_as_float/%epoch_microseconds
        └── PyDateTime                 # Python-datetime-like wrapper (strftime in templates)

src/main/resources/
├── manifest/declarative_component_schema.yaml   # copied VERBATIM from airbyte-python-cdk
├── connectors/neo/                              # Hevo platform descriptors
├── META-INF/services/                           # ServiceLoader registration
└── hevo/framework.txt                           # V2
```

## Semantics: source of truth

The reference implementation is the Python CDK's `airbyte_cdk/sources/declarative/` package.
Ports are line-faithful where practical, including quirks:

- `$ref` merges the referenced dict with sibling keys, siblings win; reference paths prefer a
  literal top-level key over traversal.
- `$parameters` materialize as fields on typed components (falsy existing values are overwritten,
  matching Python's `get(key) or value`).
- Rendered strings coerce like `ast.literal_eval`: `"3"` → long, `"03"` → string, `"True"` →
  boolean, `"true"` → string, `"{'a': 1}"` → map, `"1_000"` → 1000.
- `stream_interval`/`stream_partition` alias `stream_slice`; `stream_state` is rejected.
- The `base64binascii_decode` filter *encodes* (CDK quirk, kept).

Known divergences (accepted for now, to be caught by the P3 differential harness):

- Unknown top-level template variables fall back to the default instead of hard-failing.
- Datetime arithmetic in templates (`now_utc() - duration('P1D')`) is unsupported by jinjava's EL;
  use `day_delta` / `format_datetime` instead.
- `regex_replace` treats the replacement literally (no `\1` backreferences).
- Python-literal parsing of dict/list strings uses lenient JSON (single quotes ok; embedded
  `True`/`None` literals are not).

## Build & test

```bash
./gradlew test    # unit tests + Hevo TCK (needs VPN for the io.hevo artifact repo)
```

TCK `SchemaTests`/`DataTests` are disabled in `src/test/resources/tck/connector-acceptance-tests.yaml`
until P1/P2 give the connector real streams.
