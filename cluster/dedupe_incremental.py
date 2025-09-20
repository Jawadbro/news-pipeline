import json
import os
import pickle
from dataclasses import dataclass, field
from typing import List, Dict

from datasketch import MinHash, MinHashLSH
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# ----------------- CONFIG -----------------
LSH_THRESHOLD = 0.7
FALLBACK_THRESHOLD = 0.75
NUM_PERM = 128

INPUT_FILE = os.path.join("data", "enriched", "hn_enriched.json")
CLUSTER_FILE = os.path.join("data", "clusters", "hn_clusters.json")
LSH_INDEX_FILE = os.path.join("data", "clusters", "lsh_index.pkl")  # persistent LSH
SOURCE_WEIGHTS = {
    "hackernews": 1.0,
    "reddit": 0.8,
    "twitter": 0.5,
}  # example source weights


# ----------------- DATA STRUCTURES -----------------
@dataclass
class Cluster:
    clusterId: str
    members: List[str] = field(default_factory=list)
    representativeId: str = ""


clusters: List[Cluster] = []
story_minhashes: Dict[str, MinHash] = {}
lsh = MinHashLSH(threshold=LSH_THRESHOLD, num_perm=NUM_PERM)


# ----------------- HELPER FUNCTIONS -----------------
def create_minhash(text: str) -> MinHash:
    m = MinHash(num_perm=NUM_PERM)
    for word in set(text.lower().split()):
        m.update(word.encode("utf8"))
    return m


def compute_tfidf_similarity(text1: str, text2: str) -> float:
    vect = TfidfVectorizer().fit([text1, text2])
    tfidf_matrix = vect.transform([text1, text2])
    sim = cosine_similarity(tfidf_matrix[0], tfidf_matrix[1])[0][0]
    return sim


def load_clusters():
    global clusters, lsh, story_minhashes
    if os.path.exists(CLUSTER_FILE):
        with open(CLUSTER_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        clusters = [Cluster(**c) for c in data]

    # Load persistent LSH
    if os.path.exists(LSH_INDEX_FILE):
        with open(LSH_INDEX_FILE, "rb") as f:
            saved_lsh, saved_minhashes = pickle.load(f)
        lsh = saved_lsh
        story_minhashes = saved_minhashes


def save_clusters():
    os.makedirs(os.path.dirname(CLUSTER_FILE), exist_ok=True)
    with open(CLUSTER_FILE, "w", encoding="utf-8") as f:
        json.dump([c.__dict__ for c in clusters], f, indent=2)
    # Save LSH and MinHashes
    with open(LSH_INDEX_FILE, "wb") as f:
        pickle.dump((lsh, story_minhashes), f)


def select_representative(cluster: Cluster, story_lookup: Dict[str, dict]) -> str:
    """Select representative based on earliest timestamp and source weight."""
    members = [story_lookup[mid] for mid in cluster.members]
    # Sort by timestamp ascending, then by source weight descending
    members.sort(
        key=lambda x: (
            x.get("publishedAt") or "",
            -SOURCE_WEIGHTS.get(x.get("source", ""), 0),
        )
    )
    return members[0]["id"]


# ----------------- LOAD STORIES -----------------
with open(INPUT_FILE, "r", encoding="utf-8") as f:
    stories = json.load(f)
story_lookup = {s["id"]: s for s in stories}

# Load existing clusters and LSH index
load_clusters()

# ----------------- INCREMENTAL CLUSTERING -----------------
for story in stories:
    story_id = story["id"]
    text = story["title"] + " " + story["summary"]
    m = create_minhash(text)
    story_minhashes[story_id] = m

    near_dupes = lsh.query(m)
    added_to_cluster = False

    # Stage A: LSH
    if near_dupes:
        for rep_id in near_dupes:
            cluster = next((c for c in clusters if c.representativeId == rep_id), None)
            if cluster:
                cluster.members.append(story_id)
                cluster.representativeId = select_representative(cluster, story_lookup)
                added_to_cluster = True
                break

    # Stage B: TF-IDF fallback
    if not added_to_cluster:
        for cluster in clusters:
            rep_story = story_lookup.get(cluster.representativeId)
            if rep_story:
                sim = compute_tfidf_similarity(
                    text, rep_story["title"] + " " + rep_story["summary"]
                )
                if sim >= FALLBACK_THRESHOLD:
                    cluster.members.append(story_id)
                    cluster.representativeId = select_representative(cluster, story_lookup)
                    added_to_cluster = True
                    break

    # New cluster
    if not added_to_cluster:
        new_cluster = Cluster(
            clusterId=f"c-{len(clusters) + 1:06}",
            members=[story_id],
            representativeId=story_id,
        )
        clusters.append(new_cluster)
        lsh.insert(story_id, m)

# ----------------- SAVE RESULT -----------------
save_clusters()
print(f"✅ Incremental clustering complete. {len(clusters)} clusters saved to {CLUSTER_FILE}")
