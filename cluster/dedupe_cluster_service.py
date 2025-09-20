import json
import os
import sys

# -----------------------------
# File paths (robust handling)
# -----------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_FILE = os.path.join(PROJECT_ROOT, "data", "enriched", "hn_enriched.json")
OUTPUT_FILE = os.path.join(PROJECT_ROOT, "data", "clustered", "hn_clusters.json")

# Ensure output directory exists
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)


# -----------------------------
# Utility function
# -----------------------------
def load_input_file(file_path):
    if not os.path.exists(file_path):
        sys.exit(f"❌ Input file not found: {file_path}")
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


# -----------------------------
# Main clustering function
# -----------------------------
def run_clustering():
    # Load enriched stories
    stories = load_input_file(INPUT_FILE)

    # Placeholder: each story is its own cluster (replace with real dedupe logic)
    clusters = []
    for idx, story in enumerate(stories, start=1):
        clusters.append(
            {
                "clusterId": f"c-{idx:06d}",
                "members": [story.get("id")],
                "representativeId": story.get("id"),
            }
        )

    # Save clusters
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(clusters, f, indent=2, ensure_ascii=False)

    print(f"✅ Clustering complete: {len(clusters)} clusters saved to {OUTPUT_FILE}")


# -----------------------------
# Entry point
# -----------------------------
if __name__ == "__main__":
    run_clustering()
