#!/usr/bin/env python3
"""One-time migration of inventory ean_cache.json to tingbok formats.

Usage:
    python scripts/migrate-ean-cache.py <ean_cache.json> [--cache-dir DIR] [--manual-ean-yaml FILE]

Writes:
  - Per-EAN JSON files in the cache directory (tingbok EAN cache format)
  - Appends/merges into manual-ean.yaml (manual + supplementary data)
  - Not-found entries go into the cache directory's _not_found.json
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

SUPPLEMENTARY_KEYS = {"prices", "receipt_names", "note"}
MANUAL_PRODUCT_KEYS = {"name", "brand", "quantity", "categories", "image_url", "source", "type", "author"}


def _categories_to_list(value: object) -> list[str]:
    """Normalise categories: comma-string or list → list of stripped strings."""
    if isinstance(value, list):
        return [str(c).strip() for c in value if str(c).strip()]
    if isinstance(value, str):
        return [c.strip() for c in value.split(",") if c.strip()]
    return []


def _build_manual_entry(ean: str, entry: dict) -> dict | None:
    """Return the manual-ean.yaml fragment for this entry, or None if nothing to store."""
    result: dict = {}

    source = entry.get("source", "")

    if source == "manual":
        # Full manual product — store all product fields
        for key in MANUAL_PRODUCT_KEYS:
            if key in entry and entry[key] is not None:
                result[key] = entry[key]
        result["source"] = "manual"
        cats = _categories_to_list(entry.get("categories", []))
        if cats:
            result["categories"] = cats

    # Supplementary observations (regardless of source)
    if entry.get("prices"):
        result["prices"] = entry["prices"]

    if entry.get("lidl_receipt_name"):
        # Convert single string to observation list; infer dates from prices if available
        prices = entry.get("prices", [])
        dates = sorted(p["date"] for p in prices if p.get("date"))
        shops = list({p["shop"] for p in prices if p.get("shop")})
        obs: dict = {"name": entry["lidl_receipt_name"]}
        if shops:
            obs["shop"] = shops[0]  # best guess
        if dates:
            obs["first_seen"] = dates[0]
            obs["last_seen"] = dates[-1]
        result.setdefault("receipt_names", [])
        result["receipt_names"].append(obs)

    if entry.get("note"):
        result["note"] = entry["note"]

    return result if result else None


def _build_cache_entry(ean: str, entry: dict) -> dict:
    """Convert ean_cache.json entry to tingbok cache format."""
    result: dict = {"ean": ean}
    for key in ("name", "brand", "quantity", "image_url", "source", "type", "author"):
        if key in entry and entry[key] is not None:
            result[key] = entry[key]
    result["categories"] = _categories_to_list(entry.get("categories", []))
    return result


def _get_cache_path(cache_dir: Path, cache_key: str) -> Path:
    import hashlib

    key_hash = hashlib.sha256(cache_key.encode()).hexdigest()[:16]
    safe_key = "".join(c if c.isalnum() else "_" for c in cache_key[:50])
    return cache_dir / f"{safe_key}_{key_hash}.json"


def migrate(cache_json: Path, cache_dir: Path, manual_yaml_path: Path) -> None:
    import yaml

    data: dict = json.loads(cache_json.read_text(encoding="utf-8"))

    # Load existing manual-ean.yaml if present (to merge, not overwrite)
    manual_ean: dict = {}
    if manual_yaml_path.exists():
        with open(manual_yaml_path, encoding="utf-8") as f:
            manual_ean = yaml.safe_load(f) or {}

    not_found: list[str] = []
    cached_count = 0
    manual_count = 0

    for ean, entry in data.items():
        ean = str(ean)

        if entry is None:
            not_found.append(ean)
            continue

        # Manual entry for supplementary / source=manual data
        manual_fragment = _build_manual_entry(ean, entry)
        if manual_fragment:
            existing = manual_ean.get(ean, {})
            # Merge: new data fills in, but don't overwrite existing observations
            merged = dict(manual_fragment)
            for key in ("prices", "receipt_names"):
                if existing.get(key):
                    # Append new observations not already present
                    seen = {json.dumps(o, sort_keys=True) for o in existing[key]}
                    extras = [o for o in merged.get(key, []) if json.dumps(o, sort_keys=True) not in seen]
                    merged[key] = existing[key] + extras
            manual_ean[str(ean)] = merged
            manual_count += 1

        # Skip caching source=manual entries (no upstream data to cache)
        if entry.get("source") == "manual":
            continue

        # Write to per-file EAN cache
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_key = f"ean:{ean}"
        cache_path = _get_cache_path(cache_dir, cache_key)
        if not cache_path.exists():
            payload = {**_build_cache_entry(ean, entry), "_cached_at": time.time()}
            cache_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            cached_count += 1

    # Write not-found cache
    if not_found:
        nf_path = cache_dir / "_not_found.json"
        cache_dir.mkdir(parents=True, exist_ok=True)
        existing_nf: dict = {"entries": {}}
        if nf_path.exists():
            try:
                existing_nf = json.loads(nf_path.read_text())
            except Exception:
                pass
        for ean in not_found:
            existing_nf["entries"].setdefault(f"ean:{ean}", {"cached_at": time.time()})
        nf_path.write_text(json.dumps(existing_nf, ensure_ascii=False, indent=2), encoding="utf-8")

    # Write manual-ean.yaml
    manual_yaml_path.parent.mkdir(parents=True, exist_ok=True)
    with open(manual_yaml_path, "w", encoding="utf-8") as f:
        yaml.dump(manual_ean, f, allow_unicode=True, default_flow_style=False, sort_keys=True)

    print("Migration complete:")
    print(f"  {cached_count} entries written to cache ({cache_dir})")
    print(f"  {manual_count} entries written to manual-ean.yaml ({manual_yaml_path})")
    print(f"  {len(not_found)} not-found entries recorded")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("cache_json", type=Path, help="Path to ean_cache.json")
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path.home() / ".cache" / "tingbok" / "ean",
        help="Tingbok EAN cache directory (default: ~/.cache/tingbok/ean)",
    )
    parser.add_argument(
        "--manual-ean-yaml",
        type=Path,
        default=Path(__file__).parent.parent / "src" / "tingbok" / "data" / "manual-ean.yaml",
        help="Path to manual-ean.yaml (default: src/tingbok/data/manual-ean.yaml)",
    )
    args = parser.parse_args()
    migrate(args.cache_json, args.cache_dir, args.manual_ean_yaml)


if __name__ == "__main__":
    main()
