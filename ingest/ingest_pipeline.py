"""
Ingest Pipeline for Hacker News Stories.

This module fetches top stories from Hacker News,
normalizes them, and saves to a JSON file.
Part of the HUD LLM pipeline (ingestion stage).
"""

import json
from datetime import datetime, timezone
from pathlib import Path
import requests
from typing import List, Dict
from pydantic import BaseModel


class NewsItem(BaseModel):
    """Normalized news item schema for Hacker News."""

    title: str
    summary: str
    byline: str
    source: str
    published_at: str
    url: str
    id: int


def normalize_hn_item(item: Dict) -> Dict:
    """Normalize a Hacker News item into a standardized schema."""
    return {
        "title": item.get("title", "No title"),
        "summary": item.get("text", ""),
        "byline": item.get("by", "unknown"),
        "source": "HackerNews",
        "published_at": datetime.fromtimestamp(
            item.get("time", 0), tz=timezone.utc
        ).isoformat(),
        "url": item.get("url")
        or f"https://news.ycombinator.com/item?id={item.get('id', 0)}",
        "id": item.get("id", 0),
    }


def fetch_hn_top(n: int = 10) -> List[Dict]:
    """Fetch the top N stories from Hacker News."""
    try:
        response = requests.get(
            "https://hacker-news.firebaseio.com/v0/topstories.json", timeout=10
        )
        response.raise_for_status()
        top_ids = response.json()
        stories = []
        for story_id in top_ids[:n]:
            item_response = requests.get(
                f"https://hacker-news.firebaseio.com/v0/item/{story_id}.json",
                timeout=10,
            )
            item_response.raise_for_status()
            item = item_response.json()
            stories.append(normalize_hn_item(item))
        return stories
    except requests.RequestException as e:
        print(f"Error fetching HN stories: {e}")
        return []


def save_to_json(data: List[Dict], filename: str = "hn_stories.json") -> None:
    """Save normalized stories to a JSON file."""
    output_dir = Path("data/ingested")
    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / filename

    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"✅ Saved {len(data)} stories to {filepath}")
    except IOError as e:
        print(f"Error saving to JSON: {e}")


def main() -> None:
    """Entry point for ingestion."""
    stories = fetch_hn_top(10)
    save_to_json(stories)


if __name__ == "__main__":
    main()
