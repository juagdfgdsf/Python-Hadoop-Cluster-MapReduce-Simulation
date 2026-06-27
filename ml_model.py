import os
import json
import math
from collections import Counter

NAMENODE_FILE = "namenode.json"


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def separator(title=""):
    width = 60
    if title:
        pad = (width - len(title) - 2) // 2
        print("=" * pad + f" {title} " + "=" * pad)
    else:
        print("=" * width)


def load_namenode():
    with open(NAMENODE_FILE, "r") as f:
        return json.load(f)


def read_all_blocks(metadata):
    """Return all data rows from primary blocks."""
    all_rows = []
    for block_id, info in metadata["blocks"].items():
        path = os.path.join(info["primary_node"], info["filename"])
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or line.startswith("id,"):
                    continue
                all_rows.append(line)
    return all_rows


def parse_dataset(rows):
    """
    Convert raw CSV rows into feature vectors and labels.
    Features: [sales (float), region_code (int)]
    Label:     category string
    """
    region_map = {"North": 0, "South": 1, "East": 2, "West": 3}
    records    = []

    for row in rows:
        parts = row.split(",")
        if len(parts) < 5:
            continue
        try:
            sales  = float(parts[3].strip())
            region = region_map.get(parts[4].strip(), -1)
            cat    = parts[2].strip()
            name   = parts[1].strip()
            if region == -1:
                continue
            records.append({
                "name":     name,
                "features": [sales, region],
                "label":    cat
            })
        except ValueError:
            continue

    return records


def normalize(records):
    """Min-max normalise each feature so no single feature dominates distance."""
    if not records:
        return records

    n_features = len(records[0]["features"])
    mins = [min(r["features"][i] for r in records) for i in range(n_features)]
    maxs = [max(r["features"][i] for r in records) for i in range(n_features)]

    for r in records:
        normalised = []
        for i, val in enumerate(r["features"]):
            denom = maxs[i] - mins[i] if maxs[i] != mins[i] else 1
            normalised.append((val - mins[i]) / denom)
        r["features"] = normalised

    return records


# ─────────────────────────────────────────────────────────────────────────────
# KNN — built from scratch (no sklearn) to keep it simple and readable
# ─────────────────────────────────────────────────────────────────────────────

def euclidean_distance(a, b):
    return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))


def knn_predict(train_data, test_point_features, k=3):
    """
    Find the k nearest neighbours in train_data and return the
    majority label.
    """
    distances = []
    for record in train_data:
        dist = euclidean_distance(record["features"], test_point_features)
        distances.append((dist, record["label"]))

    # Sort by distance, take k smallest
    distances.sort(key=lambda x: x[0])
    k_labels = [label for _, label in distances[:k]]

    # Majority vote
    most_common = Counter(k_labels).most_common(1)[0][0]
    return most_common


def train_test_split(records, test_ratio=0.25):
    """Simple manual split — no shuffle so output is deterministic."""
    split_idx  = int(len(records) * (1 - test_ratio))
    train_data = records[:split_idx]
    test_data  = records[split_idx:]
    return train_data, test_data


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def run_ml():
    separator("PART 4 — Machine Learning Integration")
    print("\nAlgorithm chosen: K-Nearest Neighbors (KNN)")
    print("Features used   : Sales amount, Region (encoded)")
    print("Target label    : Product Category")

    # Load data via NameNode
    print("\nLoading data from DataNodes via NameNode metadata...")
    metadata = load_namenode()
    rows     = read_all_blocks(metadata)
    print(f"Rows loaded: {len(rows)}")

    # Parse + normalise
    records = parse_dataset(rows)
    records = normalize(records)
    print(f"Records ready for ML: {len(records)}")

    # Split
    train_data, test_data = train_test_split(records, test_ratio=0.25)
    print(f"\nTraining set : {len(train_data)} records")
    print(f"Testing set  : {len(test_data)}  records")

    # ── Training phase ───────────────────────────────────────────────────────
    separator("Training Phase")
    print("\nKNN is a lazy learner — no explicit training step.")
    print("The model stores all training records and classifies")
    print("new points by looking at their K nearest neighbours.\n")
    print(f"  K value used : 3")

    categories = list(set(r["label"] for r in train_data))
    print(f"  Classes      : {', '.join(sorted(categories))}")

    # ── Prediction phase ─────────────────────────────────────────────────────
    separator("Prediction Results")
    print()
    print(f"  {'Product':<25} {'Actual':<14} {'Predicted':<14} {'Result'}")
    print(f"  {'-'*25} {'-'*14} {'-'*14} {'-'*8}")

    correct = 0
    for record in test_data:
        predicted = knn_predict(train_data, record["features"], k=3)
        actual    = record["label"]
        status    = "✓ Correct" if predicted == actual else "✗ Wrong"
        if predicted == actual:
            correct += 1
        print(f"  {record['name']:<25} {actual:<14} {predicted:<14} {status}")

    # ── Accuracy ─────────────────────────────────────────────────────────────
    accuracy = (correct / len(test_data)) * 100 if test_data else 0
    print()
    separator("Model Accuracy")
    print()
    print(f"  Correctly classified : {correct} / {len(test_data)}")
    print(f"  Accuracy             : {accuracy:.1f}%")
    print()

    if accuracy >= 70:
        print("  The model performed reasonably well given the small dataset.")

    print()
    separator("ML Complete")
    print()


if __name__ == "__main__":
    run_ml()
