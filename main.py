import os
import json
import math
import shutil
import datetime
import hashlib

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

DATASET_FILE   = "dataset.csv"
NAMENODE_FILE  = "namenode.json"
BLOCK_SIZE     = 5          # rows per block (small so we can see multiple blocks)
REPLICATION    = 2          # how many copies each block gets (original + 1 replica)
DATANODES      = ["DataNode1", "DataNode2", "DataNode3"]


# ─────────────────────────────────────────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def separator(title=""):
    width = 60
    if title:
        pad   = (width - len(title) - 2) // 2
        print("=" * pad + f" {title} " + "=" * pad)
    else:
        print("=" * width)


def pick_different_node(primary_node, all_nodes):
    """Return a DataNode that is different from the primary one."""
    options = [n for n in all_nodes if n != primary_node]
    # Simple round-robin without random so output is reproducible
    return options[0]


def timestamp_string():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def generate_checksum(header, block_rows):
    """
    Generate a SHA-256 hash for the block content to simulate data integrity checks.
    """
    # Combine header and rows into a single string
    content = header + "\n" + "\n".join(block_rows) + "\n"
    
    # Create the SHA-256 hash object
    hash_obj = hashlib.sha256()
    
    # Update the hash object with the bytes of our content
    hash_obj.update(content.encode('utf-8'))
    
    # Return the hexadecimal string representation of the hash
    return hash_obj.hexdigest()


# ─────────────────────────────────────────────────────────────────────────────
# PART 1 — HDFS SIMULATION (block splitting + storage)
# ─────────────────────────────────────────────────────────────────────────────

def read_dataset(filepath):
    """Read the CSV and return header + list of data rows."""
    with open(filepath, "r") as f:
        lines = [line.strip() for line in f.readlines() if line.strip()]
    header = lines[0]
    rows   = lines[1:]
    return header, rows


def split_into_blocks(rows, block_size):
    """Divide rows into fixed-size blocks."""
    blocks = []
    total  = len(rows)
    num_blocks = math.ceil(total / block_size)
    for i in range(num_blocks):
        chunk = rows[i * block_size : (i + 1) * block_size]
        blocks.append(chunk)
    return blocks


def store_blocks(header, blocks, datanodes):
    """
    Write each block as a .csv file inside a DataNode folder.
    Returns a metadata dict that will go into namenode.json.
    """
    metadata = {
        "file": DATASET_FILE,
        "total_rows": sum(len(b) for b in blocks),
        "block_size": BLOCK_SIZE,
        "total_blocks": len(blocks),
        "created_at": timestamp_string(),
        "blocks": {}
    }

    for idx, block in enumerate(blocks):
        block_id  = f"block_{idx:02d}"
        # Distribute blocks across nodes in round-robin fashion
        node      = datanodes[idx % len(datanodes)]
        filename  = f"{block_id}.csv"
        filepath  = os.path.join(node, filename)

        # Write block with header so each file is self-contained
        with open(filepath, "w") as f:
            f.write(header + "\n")
            f.write("\n".join(block) + "\n")

        # Generate the hash for this block
        block_hash = generate_checksum(header, block)

        metadata["blocks"][block_id] = {
            "primary_node": node,
            "filename": filename,
            "row_count": len(block),
            "checksum": block_hash,
            "replicas": []          # filled in Part 2
        }

        print(f"  [STORED]  {block_id}  →  {node}/{filename}  ({len(block)} rows)")

    return metadata


def part1_hdfs_simulation():
    separator("PART 1 — HDFS SIMULATION")

    # Clear old DataNode contents so each run is fresh
    for node in DATANODES:
        os.makedirs(node, exist_ok=True)
        for f in os.listdir(node):
            os.remove(os.path.join(node, f))

    print(f"\nReading dataset:  {DATASET_FILE}")
    header, rows = read_dataset(DATASET_FILE)
    print(f"Total rows found: {len(rows)}")
    print(f"Block size:       {BLOCK_SIZE} rows")

    blocks = split_into_blocks(rows, BLOCK_SIZE)
    print(f"Blocks created:   {len(blocks)}\n")
    print("Distributing blocks across DataNodes...")
    print()

    metadata = store_blocks(header, blocks, DATANODES)

    print()
    print(f"Total blocks stored : {metadata['total_blocks']}")
    print(f"DataNodes used      : {', '.join(DATANODES)}")

    return header, blocks, metadata


# ─────────────────────────────────────────────────────────────────────────────
# PART 2 — DATA REPLICATION (fault tolerance)
# ─────────────────────────────────────────────────────────────────────────────

def create_replica(header, block_id, block_rows, primary_node, replica_node):
    """
    Write a replica of a block in a different DataNode.
    Replicas are NOT byte-for-byte copies — they include a replica header
    with metadata so we can tell them apart from the originals.
    """
    replica_filename = f"{block_id}_replica.csv"
    replica_path     = os.path.join(replica_node, replica_filename)

    replica_header_line = (
        f"# REPLICA  |  block={block_id}  |  "
        f"source={primary_node}  |  "
        f"stored={replica_node}  |  "
        f"created={timestamp_string()}"
    )

    with open(replica_path, "w") as f:
        f.write(replica_header_line + "\n")
        f.write(header + "\n")
        f.write("\n".join(block_rows) + "\n")

    # Generate the hash for the actual data
    replica_checksum = generate_checksum(header, block_rows)

    return replica_filename, replica_checksum


def part2_replication(header, blocks, metadata):
    separator("PART 2 — DATA REPLICATION")
    print(f"\nReplication factor: {REPLICATION}  (1 original + 1 replica)\n")

    for idx, block in enumerate(blocks):
        block_id     = f"block_{idx:02d}"
        primary_node = metadata["blocks"][block_id]["primary_node"]
        original_hash = metadata["blocks"][block_id]["checksum"]
        replica_node = pick_different_node(primary_node, DATANODES)

        replica_file, replica_hash = create_replica(header, block_id, block, primary_node, replica_node)
        
        # Simulate network integrity check
        if replica_hash != original_hash:
            print(f"  [WARNING] Checksum mismatch during transfer of {block_id}!")

        metadata["blocks"][block_id]["replicas"].append({
            "node": replica_node,
            "filename": replica_file,
            "checksum": replica_hash
        })

        print(f"  [REPLICA] {block_id}  →  {replica_node}/{replica_file}")

    print()
    print("All blocks have at least one replica stored in a different DataNode.")
    return metadata


# ─────────────────────────────────────────────────────────────────────────────
# NAMENODE — save metadata
# ─────────────────────────────────────────────────────────────────────────────

def save_namenode(metadata):
    with open(NAMENODE_FILE, "w") as f:
        json.dump(metadata, f, indent=4)
    print(f"\nNameNode metadata saved to:  {NAMENODE_FILE}")


def display_namenode_summary(metadata):
    separator("NameNode Metadata Summary")
    print(f"\n  File       : {metadata['file']}")
    print(f"  Total rows : {metadata['total_rows']}")
    print(f"  Blocks     : {metadata['total_blocks']}")
    print(f"  Created    : {metadata['created_at']}\n")

    print(f"  {'Block ID':<12} {'Primary Node':<14} {'Rows':<6} {'Checksum (Short)':<18} {'Replica Node'}")
    print(f"  {'-'*12} {'-'*14} {'-'*6} {'-'*18} {'-'*14}")

    for block_id, info in metadata["blocks"].items():
        replica_info = info["replicas"][0]["node"] if info["replicas"] else "None"
        short_hash = info['checksum'][:10] + "..." # Show just the first 10 characters
        print(f"  {block_id:<12} {info['primary_node']:<14} {info['row_count']:<6} {short_hash:<18} {replica_info}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    separator("Hadoop HDFS Simulation — Python")
    print(f"Started at: {timestamp_string()}\n")

    # Part 1 — Split and store blocks
    header, blocks, metadata = part1_hdfs_simulation()

    print()

    # Part 2 — Replicate blocks
    metadata = part2_replication(header, blocks, metadata)

    # Save NameNode JSON
    print()
    save_namenode(metadata)
    print()
    display_namenode_summary(metadata)

    print()
    separator("DONE")
    print()
    print("Next steps:")
    print("  python mapreduce.py    — run MapReduce word count & category analysis")
    print("  python ml_model.py     — train and evaluate the ML model")
    print()