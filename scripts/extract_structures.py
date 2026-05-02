"""
Extract per-GB host structures + substitutional site map from the unrelaxed
Parquet for use by the in-browser 3D viewer.

Output: data/structures/{dataset_id}/{GB}.json

  {
    "GB": "S5-RA001-S310",
    "lattice": [[a..], [b..], [c..]],          # 3x3 cartesian matrix in Å
    "host_atoms": [                             # all atoms in the host slab
        {"element": "Fe", "xyz": [x, y, z], "is_site": false, "site_label": null},
        ...
    ],
    "sites": [                                  # one entry per substitutional site
        {
            "label": "27",
            "atom_index": 26,                   # index into host_atoms
            "xyz": [x, y, z],
            "eseg_by_element": {"Mn": 0.14, "Cu": ...},
            "energy_by_element": {"Mn": -638.30, ...}
        },
        ...
    ]
  }

Run with the `pymatgen` conda env which has pyarrow + pandas.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

ROOT = Path("/home/liger/ligerzero-ai")
DATA_DIR = ROOT / "data"
OUT_DIR = DATA_DIR / "structures"

DATASETS = [
    {"id": "fe_x_gb_kp_unrelaxed", "parquet": DATA_DIR / "fe_x_gb_kp_unrelaxed.parquet"},
    # Relaxed: only summary site map (Eseg per (GB, site, element)); host slab still pulled from one structure for context.
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
    """Return the atom index whose element is not Fe (the substituent), or None."""
    for i, s in enumerate(sites):
        if s["element"] != "Fe":
            return i
    return None


def build_for_dataset(ds_id: str, parquet_path: Path):
    print(f"\n=== {ds_id} ===")
    table = pq.read_table(str(parquet_path), columns=NEEDED_COLS)
    df = table.to_pandas()
    print(f"  rows: {len(df)}")

    out_root = OUT_DIR / ds_id
    out_root.mkdir(parents=True, exist_ok=True)

    site_index = {}  # gb -> { site_label -> {atom_index, xyz} }
    host_by_gb = {}  # gb -> {lattice, host_atoms}

    for gb, gb_df in df.groupby("GB"):
        # pick the first record to establish host slab + lattice
        first = gb_df.iloc[0]
        lattice, sites = parse_structure(first["structures"])
        # replace the substituted atom back to Fe to get the host slab
        host_atoms = [{"element": s["element"], "xyz": s["xyz"]} for s in sites]
        sub_idx = find_solute_index(sites)
        if sub_idx is not None:
            host_atoms[sub_idx]["element"] = "Fe"

        host_by_gb[gb] = {
            "lattice": [[float(x) for x in row] for row in lattice],
            "host_atoms": host_atoms,
        }

        # for each (site_label) find the atom index by looking for a record with that site
        # and locating the non-Fe atom
        site_map = {}
        for site_label, site_df in gb_df.groupby("site"):
            rec = site_df.iloc[0]
            _, srec_sites = parse_structure(rec["structures"])
            idx = find_solute_index(srec_sites)
            if idx is None:
                continue
            site_map[str(site_label)] = {
                "atom_index": idx,
                "xyz": srec_sites[idx]["xyz"],
            }
        site_index[gb] = site_map

        print(f"  {gb}: {len(host_atoms)} atoms, {len(site_map)} sub. sites")

    # Build per-GB output objects
    for gb, host in host_by_gb.items():
        sites_payload = []
        for label, info in sorted(site_index[gb].items(), key=lambda kv: int(kv[0]) if kv[0].isdigit() else kv[0]):
            # collect Eseg per element for this site
            mask = (df["GB"] == gb) & (df["site"] == label)
            sub = df.loc[mask, ["element", "Eseg", "energy", "convergence"]].copy()
            eseg_by_elem = {}
            energy_by_elem = {}
            for _, r in sub.iterrows():
                elem = r["element"]
                if pd.notna(r["Eseg"]):
                    eseg_by_elem[elem] = round(float(r["Eseg"]), 4)
                if pd.notna(r["energy"]):
                    energy_by_elem[elem] = round(float(r["energy"]), 4)

            sites_payload.append({
                "label": label,
                "atom_index": info["atom_index"],
                "xyz": [round(x, 4) for x in info["xyz"]],
                "eseg_by_element": eseg_by_elem,
                "energy_by_element": energy_by_elem,
            })

        # Mark host atoms: which indices are sub. sites
        host_atoms_out = []
        sub_indices = {s["atom_index"]: s["label"] for s in sites_payload}
        for i, a in enumerate(host["host_atoms"]):
            host_atoms_out.append({
                "element": a["element"],
                "xyz": [round(x, 4) for x in a["xyz"]],
                "is_site": i in sub_indices,
                "site_label": sub_indices.get(i),
            })

        payload = {
            "GB": gb,
            "dataset_id": ds_id,
            "lattice": [[round(x, 4) for x in row] for row in host["lattice"]],
            "host_atoms": host_atoms_out,
            "sites": sites_payload,
        }
        out_path = out_root / f"{gb}.json"
        with open(out_path, "w") as f:
            json.dump(payload, f, separators=(",", ":"))
        kb = out_path.stat().st_size / 1024
        print(f"    -> {out_path.name} ({kb:.1f} KB)")


def build_gb_index():
    """Top-level index: list of GBs per dataset with summary stats."""
    idx = {}
    for ds in DATASETS:
        ds_id = ds["id"]
        ds_dir = OUT_DIR / ds_id
        if not ds_dir.exists():
            continue
        gbs = []
        for p in sorted(ds_dir.glob("*.json")):
            with open(p) as f:
                payload = json.load(f)
            esegs = []
            for s in payload["sites"]:
                esegs.extend(s["eseg_by_element"].values())
            gbs.append({
                "GB": payload["GB"],
                "n_atoms": len(payload["host_atoms"]),
                "n_sites": len(payload["sites"]),
                "n_elements": len({e for s in payload["sites"] for e in s["eseg_by_element"]}),
                "Eseg_min": round(min(esegs), 4) if esegs else None,
                "Eseg_max": round(max(esegs), 4) if esegs else None,
            })
        idx[ds_id] = gbs
    out = OUT_DIR / "index.json"
    with open(out, "w") as f:
        json.dump(idx, f, indent=2)
    print(f"\nWrote {out}")


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for ds in DATASETS:
        if not ds["parquet"].exists():
            print(f"SKIP {ds['id']}: parquet not found at {ds['parquet']}")
            continue
        build_for_dataset(ds["id"], ds["parquet"])
    build_gb_index()


if __name__ == "__main__":
    main()
