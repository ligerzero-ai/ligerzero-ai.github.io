"""
Convert pickle files to multi-dataset-aware JSON for the Data Explorer.

Output structure:
  data/element_index.json  — per-element summary with per-dataset breakdowns
  data/elements/{Symbol}.json — per-element detail records tagged with dataset_id

Each pickle file is registered as a dataset with a human-readable name and ID.
New datasets can be added by appending to the DATASETS list.
"""

import pickle
import json
import os
import numpy as np
import pandas as pd

OUTPUT_DIR = "/home/liger/ligerzero-ai/data"

# ──────────────────────────────────────────────────────────────────────
# Dataset registry — add new pickle sources here
# ──────────────────────────────────────────────────────────────────────
DATASETS = [
    {
        "id": "fe_x_gb_kp_unrelaxed",
        "name": "Fe-X GB Segregation (KP, Unrelaxed)",
        "category": "Fe-X GB Segregation",
        "path": "/mnt/c/Users/liger/Downloads/KP_Unrelaxed_Substitutional_with_Eseg.pkl",
        "method": "KP (DFT, VASP/PAW-PBE)",
        "description": "Unrelaxed substitutional solute segregation in Fe grain boundaries",
    },
    {
        "id": "fe_x_gb_kp_relaxed",
        "name": "Fe-X GB Segregation (KP, Relaxed)",
        "category": "Fe-X GB Segregation",
        "path": "/mnt/c/Users/liger/Downloads/KP_Relaxed_Substitutional_with_Eseg.pkl",
        "method": "KP (DFT, VASP/PAW-PBE)",
        "description": "Relaxed substitutional solute segregation in Fe grain boundaries",
    },
]

# Columns to keep in the lightweight per-element detail JSON
DETAIL_COLS = [
    "job_name", "GB", "element", "site", "Z", "n_Fe", "n_Fe_diff",
    "energy", "energy_zero", "Eseg", "scf_steps", "convergence",
    "element_count", "potcar_electron_count", "total_electron_count",
    "method", "state", "defect_type", "dataset_case",
]


def load_datasets():
    """Load all registered datasets and tag each row with dataset_id."""
    frames = []
    dataset_meta = []
    for ds in DATASETS:
        path = ds["path"]
        if not os.path.exists(path):
            print(f"  SKIP (not found): {path}")
            continue
        print(f"Loading {ds['id']} from {os.path.basename(path)}...")
        with open(path, "rb") as f:
            df = pickle.load(f)
        df["dataset_id"] = ds["id"]
        df["dataset_name"] = ds["name"]
        df["dataset_category"] = ds["category"]
        frames.append(df)
        dataset_meta.append({
            "id": ds["id"],
            "name": ds["name"],
            "category": ds["category"],
            "method": ds["method"],
            "description": ds["description"],
            "n_rows": len(df),
            "n_elements": int(df["element"].nunique()),
        })
        print(f"  -> {df.shape[0]} rows, {df.shape[1]} cols")

    combined = pd.concat(frames, ignore_index=True)
    print(f"Combined: {combined.shape[0]} rows across {len(dataset_meta)} datasets")
    return combined, dataset_meta


def build_element_index(df, dataset_meta):
    """
    Build a per-element summary with per-dataset breakdowns.
    Structure:
      { "Cu": {
          "symbol": "Cu", "Z": 29,
          "total_records": 108,
          "datasets": {
              "fe_x_gb_kp_unrelaxed": { ... summary ... },
              "fe_x_gb_kp_relaxed": { ... summary ... },
          }
        }, ... }
    """
    index = {}
    for element, el_grp in df.groupby("element"):
        ds_summaries = {}
        for ds_id, ds_grp in el_grp.groupby("dataset_id"):
            ds_summaries[ds_id] = _summarize_group(ds_grp)

        index[element] = {
            "symbol": element,
            "Z": int(el_grp["Z"].iloc[0]),
            "total_records": len(el_grp),
            "n_datasets": len(ds_summaries),
            "datasets": ds_summaries,
        }
    return index


def _summarize_group(grp):
    """Compute summary statistics for a group of records."""
    summary = {
        "count": len(grp),
        "converged_frac": round(float(grp["convergence"].mean()), 4),
    }
    # Eseg stats (if present and not all NaN)
    if "Eseg" in grp.columns and grp["Eseg"].notna().any():
        summary["Eseg_mean"] = round(float(grp["Eseg"].mean()), 4)
        summary["Eseg_min"] = round(float(grp["Eseg"].min()), 4)
        summary["Eseg_max"] = round(float(grp["Eseg"].max()), 4)
        summary["Eseg_std"] = round(float(grp["Eseg"].std()), 4) if len(grp) > 1 else 0.0
    # GB-specific fields
    if "GB" in grp.columns:
        summary["grain_boundaries"] = sorted(grp["GB"].unique().tolist())
        summary["n_grain_boundaries"] = int(grp["GB"].nunique())
    if "site" in grp.columns:
        summary["n_sites"] = int(grp["site"].nunique())
    if "state" in grp.columns:
        for st in grp["state"].unique():
            summary[f"n_{st.lower()}"] = int((grp["state"] == st).sum())
    return summary


def build_element_details(df):
    """
    Build per-element detail data with dataset_id on each record.
    """
    keep_cols = DETAIL_COLS + ["dataset_id", "dataset_name", "dataset_category"]
    # Only keep columns that exist
    keep_cols = [c for c in keep_cols if c in df.columns]

    details = {}
    for element, grp in df.groupby("element"):
        rows = grp[keep_cols].copy()
        records = rows.to_dict(orient="records")
        for rec in records:
            for k, v in rec.items():
                if isinstance(v, (np.integer,)):
                    rec[k] = int(v)
                elif isinstance(v, (np.floating,)):
                    rec[k] = round(float(v), 6) if not np.isnan(v) else None
                elif isinstance(v, (np.bool_,)):
                    rec[k] = bool(v)
        details[element] = records
    return details


def build_parquet(df):
    """Export full data as Parquet with heavy columns serialized."""
    export = df.copy()
    for col in ["forces", "stresses", "magmoms"]:
        if col in export.columns:
            export[col] = export[col].apply(
                lambda x: json.dumps(x.tolist()) if isinstance(x, np.ndarray) else None
            )
    for col in ["kpoints", "incar"]:
        if col in export.columns:
            export[col] = export[col].apply(
                lambda x: json.dumps(x) if isinstance(x, dict) else None
            )
    return export


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(os.path.join(OUTPUT_DIR, "elements"), exist_ok=True)

    df, dataset_meta = load_datasets()

    # 1. Dataset registry (so the frontend knows what datasets exist)
    meta_path = os.path.join(OUTPUT_DIR, "datasets.json")
    with open(meta_path, "w") as f:
        json.dump(dataset_meta, f, indent=2)
    print(f"\nDataset registry: {meta_path} ({len(dataset_meta)} datasets)")

    # 2. Element index with per-dataset breakdowns
    print("Building element index...")
    index = build_element_index(df, dataset_meta)
    index_path = os.path.join(OUTPUT_DIR, "element_index.json")
    with open(index_path, "w") as f:
        json.dump(index, f, indent=2)
    size_kb = os.path.getsize(index_path) / 1024
    print(f"  -> {index_path} ({size_kb:.1f} KB, {len(index)} elements)")

    # 3. Per-element detail JSON files
    print("Building per-element detail files...")
    details = build_element_details(df)
    for element, records in details.items():
        el_path = os.path.join(OUTPUT_DIR, "elements", f"{element}.json")
        with open(el_path, "w") as f:
            json.dump(records, f)
    total_size = sum(
        os.path.getsize(os.path.join(OUTPUT_DIR, "elements", f))
        for f in os.listdir(os.path.join(OUTPUT_DIR, "elements"))
    )
    print(f"  -> {len(details)} element files ({total_size / 1024 / 1024:.1f} MB total)")

    # 4. Full Parquet files (per dataset)
    try:
        import pyarrow  # noqa: F401
        print("\nBuilding Parquet files...")
        for ds in DATASETS:
            ds_id = ds["id"]
            subset = df[df["dataset_id"] == ds_id]
            if len(subset) == 0:
                continue
            export = build_parquet(subset)
            pq_path = os.path.join(OUTPUT_DIR, f"{ds_id}.parquet")
            export.to_parquet(pq_path, index=False, engine="pyarrow")
            size_mb = os.path.getsize(pq_path) / 1024 / 1024
            print(f"  -> {pq_path} ({size_mb:.1f} MB, {len(subset)} rows)")
    except ImportError:
        print("\nSkipping Parquet export (pyarrow not installed).")

    print("\nDone! Output in:", OUTPUT_DIR)


if __name__ == "__main__":
    main()
