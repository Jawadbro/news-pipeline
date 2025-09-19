import json
from datetime import datetime, timezone
from pathlib import Path

import requests


# --- Normalized schema template ---
def normalize_hn_item(item: dict) -> dict:
    """Normalize a HackerNews item to our standard schema."""
    item_id = item.get("id", "")
    return {
        "title": item.get("title"),
        "summary": item.get("text") or "",
        "byline": item.get("by", "unknown"),
        "source": "HackerNews",
        "publishedAt": datetime.fromtimestamp(
            item.get("time", 0), tz=timezone.utc
        ).isoformat(),
        "url": (
            item.get("url") or f"https://news.ycombinator.com/item?id={item_id}"
        ),
        "id": item_id,
    }


# --- Fetch Top Stories ---
def fetch_hn_top(n=10):
    """Fetch top N stories from HackerNews."""
    response = requests.get(
        "https://hacker-news.firebaseio.com/v0/topstories.json"
    )
    top_ids = response.json()
    stories = []
    for story_id in top_ids[:n]:
        item_response = requests.get(
            f"https://hacker-news.firebaseio.com/v0/item/{story_id}.json"
        )
        item = item_response.json()
        stories.append(normalize_hn_item(item))
    return stories


# --- Save to file ---
def save_to_json(data, filename="hn_stories.json"):
    """Save data to JSON file in the ingested directory."""
    output_dir = Path("data/ingested")
    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / filename

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"✅ Saved {len(data)} stories to {filepath}")


if __name__ == "__main__":
    stories = fetch_hn_top(10)  # Fetch 10 top stories
    save_to_json(stories)