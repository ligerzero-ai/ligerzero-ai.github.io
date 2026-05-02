"""
Extract per-GB host structures + substitutional site maps for the in-browser
3D viewer.

Output: data/structures/{GB}.json — ONE file per grain boundary, shared by all
datasets. Coordinates always come from the *unrelaxed* parquet (it's the
canonical reference geometry); each dataset contributes its own Eseg/energy
overlay so the viewer can swap maps without re-fetching.

  {
    "GB": "S5-RA001-S310",
    "lattice": [[a..], [b..], [c..]],
    "host_atoms": [
        {"element": "Fe", "xyz": [x, y, z], "is_site": false, "site_label": null},
        ...
    ],
    "sites": [
        {
            "label": "27",
            "atom_index": 26,
            "xyz": [x, y, z],
            "datasets": {
                "fe_x_gb_kp_unrelaxed": {
                    "eseg_by_element": {"Mn": 0.14, "Cu": ...},
                    "energy_by_element": {"Mn": -638.30, ...}
                },
                "fe_x_gb_kp_relaxed": { ... }
            }
        },
        ...
    ]
  }

Run with the `pymatgen` conda env which has pyarrow + pandas.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

ROOT = Path("/home/liger/ligerzero-ai")
DATA_DIR = ROOT / "data"
OUT_DIR = DATA_DIR / "structures"

# Which parquet provides the canonical host slab geometry. The viewer always
# shows the unrelaxed reference, even when the user is looking at the relaxed
# dataset's Eseg overlay.
GEOMETRY_SOURCE = "fe_x_gb_kp_unrelaxed"

DATASETS = [
    {"id": "fe_x_gb_kp_unrelaxed", "parquet": DATA_DIR / "fe_x_gb_kp_unrelaxed.parquet"},
    {"id": "fe_x_gb_kp_relaxed",   "parquet": DATA_DIR / "fe_x_gb_kp_relaxed.parquet"},
]

NEEDED_COLS = ["structures", "GB", "site", "element", "Eseg", "energy", "convergence"]


def parse_structure(struct_json: str):
    s = json.loads(struct_json)
    lattice = s["lattice"]["matrix"]
    sites = []
    for site in s["sites"]:
        elem = site["species"][0]["element"]
        xyz = site["xyz"]
        sites.append({"element": elem, "xyz": [float(x) for x in xyz]})
    return lattice, sites


def find_solute_index(sites: list[dict]) -> int | None:
    for i, s in enumerate(sites):
        if s["element"] != "Fe":
            return i
    return None


def load_dataset(parquet_path: Path) -> pd.DataFrame:
    return pq.read_table(str(parquet_path), columns=NEEDED_COLS).to_pandas()


def build_geometry(df: pd.DataFrame):
    """From the unrelaxed dataframe, derive per-GB host slab + site index map."""
    geometry = {}  # gb -> { lattice, host_atoms, sites: [{label, atom_index, xyz}] }
    for gb, gb_df in df.groupby("GB"):
        first = gb_df.iloc[0]
        lattice, sites = parse_structure(first["structures"])
        host_atoms = [{"element": s["element"], "xyz": s["xyz"]} for s in sites]
        sub_idx = find_solute_index(sites)
        if sub_idx is not None:
            host_atoms[sub_idx]["element"] = "Fe"

        # Map each substitutional site label to its atom index by inspecting
        # one record per (GB, site).
        site_map = {}
        for label, sd in gb_df.groupby("site"):
            rec = sd.iloc[0]
            _, srec_sites = parse_structure(rec["structures"])
            idx = find_solute_index(srec_sites)
            if idx is None:
                continue
            site_map[str(label)] = {
                "atom_index": idx,
                "xyz": srec_sites[idx]["xyz"],
            }

        geometry[gb] = {"lattice": lattice, "host_atoms": host_atoms, "site_map": site_map}
    return geometry


def collect_eseg_overlay(df: pd.DataFrame, gb: str, site_label: str):
    """Per (GB, site), gather Eseg & energy for every solute element in this dataset."""
    mask = (df["GB"] == gb) & (df["site"] == site_label)
    sub = df.loc[mask, ["element", "Eseg", "energy"]]
    eseg_by_elem = {}
    energy_by_elem = {}
    for _, r in sub.iterrows():
        elem = r["element"]
        if pd.notna(r["Eseg"]):
            eseg_by_elem[elem] = round(float(r["Eseg"]), 4)
        if pd.notna(r["energy"]):
            energy_by_elem[elem] = round(float(r["energy"]), 4)
    return eseg_by_elem, energy_by_elem


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Load all datasets up front
    frames = {}
    for ds in DATASETS:
        if not ds["parquet"].exists():
            print(f"SKIP {ds['id']}: {ds['parquet']} not found")
            continue
        print(f"Loading {ds['id']}...")
        frames[ds["id"]] = load_dataset(ds["parquet"])
        print(f"  rows: {len(frames[ds['id']])}")

    if GEOMETRY_SOURCE not in frames:
        raise RuntimeError(f"Geometry source dataset '{GEOMETRY_SOURCE}' is missing.")

    # 2. Derive canonical geometry from the unrelaxed parquet
    print(f"\nDeriving geometry from {GEOMETRY_SOURCE}...")
    geometry = build_geometry(frames[GEOMETRY_SOURCE])
    for gb, geo in geometry.items():
        print(f"  {gb}: {len(geo['host_atoms'])} atoms, {len(geo['site_map'])} sub. sites")

    # 3. For each GB, build the unified payload
    summary_idx = []  # for index.json
    for gb, geo in geometry.items():
        sites_payload = []
        for label, info in sorted(geo["site_map"].items(), key=lambda kv: int(kv[0]) if kv[0].isdigit() else kv[0]):
            datasets_overlay = {}
            for ds_id, df in frames.items():
                eseg_by_elem, energy_by_elem = collect_eseg_overlay(df, gb, label)
                datasets_overlay[ds_id] = {
                    "eseg_by_element": eseg_by_elem,
                    "energy_by_element": energy_by_elem,
                }
            sites_payload.append({
                "label": label,
                "atom_index": info["atom_index"],
                "xyz": [round(x, 4) for x in info["xyz"]],
                "datasets": datasets_overlay,
            })

        # Tag host atoms with site flags for convenient client-side iteration
        sub_indices = {s["atom_index"]: s["label"] for s in sites_payload}
        host_atoms_out = []
        for i, a in enumerate(geo["host_atoms"]):
            host_atoms_out.append({
                "element": a["element"],
                "xyz": [round(x, 4) for x in a["xyz"]],
                "is_site": i in sub_indices,
                "site_label": sub_indices.get(i),
            })

        payload = {
            "GB": gb,
            "geometry_source": GEOMETRY_SOURCE,
            "lattice": [[round(x, 4) for x in row] for row in geo["lattice"]],
            "host_atoms": host_atoms_out,
            "sites": sites_payload,
        }
        out_path = OUT_DIR / f"{gb}.json"
        with open(out_path, "w") as f:
            json.dump(payload, f, separators=(",", ":"))
        kb = out_path.stat().st_size / 1024
        print(f"  -> {out_path.name} ({kb:.1f} KB)")

        # Per-dataset summary stats for this GB
        per_ds = {}
        for ds_id in frames:
            esegs = []
            for s in sites_payload:
                esegs.extend(s["datasets"][ds_id]["eseg_by_element"].values())
            per_ds[ds_id] = {
                "n_sites": len(sites_payload),
                "n_elements": len({e for s in sites_payload for e in s["datasets"][ds_id]["eseg_by_element"]}),
                "Eseg_min": round(min(esegs), 4) if esegs else None,
                "Eseg_max": round(max(esegs), 4) if esegs else None,
            }
        summary_idx.append({
            "GB": gb,
            "n_atoms": len(host_atoms_out),
            "n_sites": len(sites_payload),
            "datasets": per_ds,
        })

    # 4. Write the index. Group by dataset for backwards compatibility / discovery.
    index_payload = {
        "geometry_source": GEOMETRY_SOURCE,
        "grain_boundaries": summary_idx,
    }
    # Also expose per-dataset GB lists for convenience
    for ds_id in frames:
        index_payload.setdefault("by_dataset", {})[ds_id] = [
            {
                "GB": g["GB"],
                "n_atoms": g["n_atoms"],
                "n_sites": g["datasets"][ds_id]["n_sites"],
                "n_elements": g["datasets"][ds_id]["n_elements"],
                "Eseg_min": g["datasets"][ds_id]["Eseg_min"],
                "Eseg_max": g["datasets"][ds_id]["Eseg_max"],
            }
            for g in summary_idx
        ]

    idx_path = OUT_DIR / "index.json"
    with open(idx_path, "w") as f:
        json.dump(index_payload, f, indent=2)
    print(f"\nWrote {idx_path}")


if __name__ == "__main__":
    main()
