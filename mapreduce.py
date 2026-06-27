import os
import json
import hashlib
from collections import defaultdict

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


def verify_and_read_block(node, filename, expected_hash):
    """
    Reads the block, verifies its integrity against the expected hash, 
    and returns the clean data rows.
    Returns (rows, is_valid_boolean).
    """
    path = os.path.join(node, filename)
    
    # 1. Try to read the file
    try:
        with open(path, "r") as f:
            raw_lines = f.readlines()
    except FileNotFoundError:
        return None, False  # File is completely missing
        
    # 2. Reconstruct the raw data exactly as it was originally hashed 
    # (ignoring the '# REPLICA' comment line if we are reading a backup)
    data_lines = [line for line in raw_lines if not line.startswith("#")]
    content = "".join(data_lines)
    
    # 3. Verify the Checksum
    hash_obj = hashlib.sha256()
    hash_obj.update(content.encode('utf-8'))
    actual_hash = hash_obj.hexdigest()
    
    if actual_hash != expected_hash:
        return None, False  # Data is corrupted!
        
    # 4. Extract just the pure data rows (skip the CSV header)
    rows = []
    for line in data_lines:
        line = line.strip()
        if not line or line.startswith("id,"):
            continue
        rows.append(line)
        
    return rows, True


def collect_all_rows(metadata):
    """
    Walk NameNode metadata. Try to read primary blocks. 
    If corrupted or missing, fallback to the replica.
    """
    all_rows = []
    for block_id, info in metadata["blocks"].items():
        primary_node  = info["primary_node"]
        filename      = info["filename"]
        expected_hash = info["checksum"]
        
        # Try fetching from the Primary Node first
        rows, is_valid = verify_and_read_block(primary_node, filename, expected_hash)
        
        if is_valid:
            all_rows.extend(rows)
        else:
            # --- FAULT TOLERANCE RECOVERY KICKS IN HERE ---
            print(f"  [RECOVERY] Primary block {block_id} in {primary_node} is missing/corrupted!")
            
            if info["replicas"]:
                replica = info["replicas"][0]
                print(f"             Fetching backup from {replica['node']}...")
                
                r_rows, r_valid = verify_and_read_block(replica["node"], replica["filename"], replica["checksum"])
                
                if r_valid:
                    all_rows.extend(r_rows)
                    print(f"             ✓ Successfully recovered {block_id}!")
                else:
                    print(f"  [FATAL]    Replica for {block_id} is ALSO corrupted!")
            else:
                print(f"  [FATAL]    No replicas exist for {block_id}!")
                
    return all_rows


def parse_row(row):
    """
    Parse a CSV row into a dict.
    Columns: id, name, category, sales, region
    """
    parts = row.split(",")
    if len(parts) < 5:
        return None
    return {
        "id":       parts[0].strip(),
        "name":     parts[1].strip(),
        "category": parts[2].strip(),
        "sales":    parts[3].strip(),
        "region":   parts[4].strip(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# TASK 1 — WORD COUNT
# ─────────────────────────────────────────────────────────────────────────────

def map_word_count(rows):
    """
    MAP phase: emit (word, 1) for every word in the product 'name' column.
    """
    pairs = []
    for row in rows:
        record = parse_row(row)
        if not record:
            continue
        words = record["name"].lower().split()
        for word in words:
            # Clean punctuation
            word = word.strip(".,!?\"'")
            if word:
                pairs.append((word, 1))
    return pairs


def shuffle_word_count(pairs):
    """
    SHUFFLE phase: group all values by key.
    """
    grouped = defaultdict(list)
    for word, count in pairs:
        grouped[word].append(count)
    return dict(grouped)


def reduce_word_count(grouped):
    """
    REDUCE phase: sum up counts for each word.
    """
    result = {}
    for word, counts in grouped.items():
        result[word] = sum(counts)
    return result


# ─────────────────────────────────────────────────────────────────────────────
# TASK 2 — CATEGORY FREQUENCY
# ─────────────────────────────────────────────────────────────────────────────

def map_category(rows):
    """MAP phase: emit (category, 1) for each record."""
    pairs = []
    for row in rows:
        record = parse_row(row)
        if not record:
            continue
        pairs.append((record["category"], 1))
    return pairs


def shuffle_group(pairs):
    """Generic shuffle — works for any (key, value) pairs."""
    grouped = defaultdict(list)
    for key, value in pairs:
        grouped[key].append(value)
    return dict(grouped)


def reduce_sum(grouped):
    """Generic reduce — sums integer values per key."""
    return {k: sum(v) for k, v in grouped.items()}


# ─────────────────────────────────────────────────────────────────────────────
# TASK 3 — REGION SALES
# ─────────────────────────────────────────────────────────────────────────────

def map_region_sales(rows):
    """MAP phase: emit (region, sales) for each record."""
    pairs = []
    for row in rows:
        record = parse_row(row)
        if not record:
            continue
        try:
            sales = int(record["sales"])
        except ValueError:
            continue
        pairs.append((record["region"], sales))
    return pairs


# ─────────────────────────────────────────────────────────────────────────────
# DISPLAY HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def print_table(title, data_dict, col1="Key", col2="Count", sort_by_value=True):
    print(f"\n  {title}")
    print(f"  {'-'*40}")
    sorted_items = sorted(data_dict.items(), key=lambda x: x[1], reverse=sort_by_value)
    for key, value in sorted_items:
        bar = "█" * min(value, 30)
        print(f"  {key:<22} {str(value):<6}  {bar}")
    print()


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def run_mapreduce():
    separator("PART 3 — MapReduce Implementation")

    # Load block locations from NameNode
    print("\nLoading block metadata from NameNode...")
    metadata = load_namenode()
    print(f"Blocks found: {metadata['total_blocks']}")

    # Collect all rows from all blocks (with fault tolerance!)
    all_rows = collect_all_rows(metadata)
    print(f"Total data rows read: {len(all_rows)}")

    # ── Task 1: Word Count ──────────────────────────────────────────────────
    separator("Task 1 — Word Count")
    print("\n  [MAP]     Emitting (word, 1) pairs from product names...")
    wc_mapped   = map_word_count(all_rows)
    print(f"            Total pairs emitted: {len(wc_mapped)}")

    print("  [SHUFFLE] Grouping pairs by word...")
    wc_shuffled = shuffle_word_count(wc_mapped)
    print(f"            Unique words found: {len(wc_shuffled)}")

    print("  [REDUCE]  Summing counts per word...")
    wc_result   = reduce_word_count(wc_shuffled)

    print_table("Word Frequency (product names)", wc_result, sort_by_value=True)

    # ── Task 2: Category Frequency ──────────────────────────────────────────
    separator("Task 2 — Category Frequency")
    print("\n  [MAP]     Emitting (category, 1) pairs...")
    cat_mapped   = map_category(all_rows)
    print(f"            Total pairs emitted: {len(cat_mapped)}")

    print("  [SHUFFLE] Grouping by category...")
    cat_shuffled = shuffle_group(cat_mapped)

    print("  [REDUCE]  Counting products per category...")
    cat_result   = reduce_sum(cat_shuffled)

    print_table("Product Count by Category", cat_result)

    # ── Task 3: Region Sales ─────────────────────────────────────────────────
    separator("Task 3 — Total Sales by Region")
    print("\n  [MAP]     Emitting (region, sales) pairs...")
    reg_mapped   = map_region_sales(all_rows)
    print(f"            Total pairs emitted: {len(reg_mapped)}")

    print("  [SHUFFLE] Grouping by region...")
    reg_shuffled = shuffle_group(reg_mapped)

    print("  [REDUCE]  Summing sales per region...")
    reg_result   = reduce_sum(reg_shuffled)

    print_table("Total Sales by Region", reg_result)

    separator("MapReduce Complete")
    print()
    return cat_result, reg_result


if __name__ == "__main__":
    run_mapreduce()