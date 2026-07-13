#!/usr/bin/env python3
"""Generate a Hevo connector registration from an Airbyte declarative manifest.

The manifest is self-describing: ``spec.connection_specification`` declares the
credential fields (name, title, description, airbyte_secret, required, order),
and the top-level ``description`` describes the source. This script turns that
into the resources ``h20 register-local-connectors`` consumes, so each manifest
becomes its own first-class Hevo source riding the shared neo runtime image:

  src/main/resources/manifests/<id>.yaml            bundled manifest (runtime)
  src/main/resources/connectors/<id>/metadata.yaml
  src/main/resources/connectors/<id>/source_configuration.yaml
  src/main/resources/connectors/<id>/object_configuration.yaml   (copied from neo)

Usage:
  python3 tools/generate_registration.py <manifest.yaml> --id ordergroove_dsl \
      --display-name Ordergroove

After generating, rebuild + register: ./build-docker-local.sh
"""

import argparse
import shutil
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
RESOURCES = REPO_ROOT / "src" / "main" / "resources"
NEO_LOGO = "https://res.cloudinary.com/hevo/image/upload/v1764867647/dashboard/image_iwkgyj.png"


def build_metadata(manifest: dict, connector_id: str, display_name: str) -> dict:
    description = (manifest.get("description") or f"{display_name} (declarative)").strip()
    # Hermes caps description length (varchar 250)
    if len(description) > 240:
        description = description[:237] + "..."
    return {
        "type": connector_id.upper(),
        "standard_type": connector_id.upper(),
        "version": "1.0.0",
        "display_name": display_name,
        "logo_url": NEO_LOGO,
        "dark_mode_logo_url": NEO_LOGO,
        "description": description,
        "documentation_path": f"/sources/{connector_id}/",
        "in_beta": True,
        "categories": ["SALES_AND_SUPPORT"],
        "attributes": {
            "supports_object_relations": False,
            "supported_load_modes": ["MERGE"],
            "replication_types": ["HISTORICAL_AND_INCREMENTAL"],
        },
    }


def build_source_configuration(manifest: dict, display_name: str) -> dict:
    spec = (manifest.get("spec") or {}).get("connection_specification") or {}
    properties: dict = spec.get("properties") or {}
    required = set(spec.get("required") or [])

    ordered = sorted(properties.items(), key=lambda kv: kv[1].get("order", 999))

    fields = []
    for field_order, (name, prop) in enumerate(ordered, start=1):
        is_secret = bool(prop.get("airbyte_secret"))
        field = {
            "name": name,
            "display_name": prop.get("title") or name.replace("_", " ").title(),
            "type": "PASSWORD" if is_secret else "STRING",
            "required": name in required,
            "is_eligible_for_edit": True,
            "group": {"type": "CONNECTION", "field_order": field_order},
            "importance": "HIGH",
            "size": 100,
        }
        description = prop.get("description")
        if description:
            field["description"] = description
            field["helper_text"] = {"text": description}
        if "default" in prop:
            field["default_value"] = prop["default"]
        fields.append(field)

    return {
        "groups": [
            {
                "type": "CONNECTION",
                "order": 1,
                "title": f"Connect to your {display_name} account",
            }
        ],
        "fields": fields,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--id", required=True, dest="connector_id",
                        help="connector id to register (lowercase, e.g. ordergroove_dsl)")
    parser.add_argument("--display-name", required=True)
    args = parser.parse_args()

    manifest_text = args.manifest.read_text()
    manifest = yaml.safe_load(manifest_text)
    if manifest.get("type") != "DeclarativeSource":
        raise SystemExit(f"{args.manifest} is not a DeclarativeSource manifest")

    manifests_dir = RESOURCES / "manifests"
    manifests_dir.mkdir(exist_ok=True)
    (manifests_dir / f"{args.connector_id}.yaml").write_text(manifest_text)

    connector_dir = RESOURCES / "connectors" / args.connector_id
    connector_dir.mkdir(parents=True, exist_ok=True)

    metadata = build_metadata(manifest, args.connector_id, args.display_name)
    (connector_dir / "metadata.yaml").write_text(
        yaml.safe_dump(metadata, sort_keys=False, allow_unicode=True))

    source_configuration = build_source_configuration(manifest, args.display_name)
    (connector_dir / "source_configuration.yaml").write_text(
        yaml.safe_dump(source_configuration, sort_keys=False, allow_unicode=True))

    shutil.copyfile(RESOURCES / "connectors" / "neo" / "object_configuration.yaml",
                    connector_dir / "object_configuration.yaml")

    field_names = [f["name"] for f in source_configuration["fields"]]
    print(f"Generated {connector_dir.relative_to(REPO_ROOT)}")
    print(f"  bundled manifest: src/main/resources/manifests/{args.connector_id}.yaml")
    print(f"  form fields: {field_names}")
    print("Now run ./build-docker-local.sh to build + register.")


if __name__ == "__main__":
    main()
