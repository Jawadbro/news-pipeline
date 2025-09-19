import json
import os
import time
import requests
from bs4 import BeautifulSoup
from langdetect import detect, DetectorFactory

# Ensure consistent language detection
DetectorFactory.seed = 0

INPUT_FILE = os.path.join("data", "ingested", "hn_stories.json")
OUTPUT_FILE = os.path.join("data", "extracted", "hn_clean.json")


def clean_text(text: str) -> str:
    """Remove HTML tags and normalize whitespace."""
    if not text:
        return ""
    soup = BeautifulSoup(text, "html.parser")
    cleaned = soup.get_text(separator=" ", strip=True)
    return " ".join(cleaned.split())


def fetch_summary(url: str, retries: int = 3) -> str:
    """Fetch first few paragraphs from an external URL with retries and SSL ignore."""
    if not url:
        return ""

    headers = {"User-Agent": "HN-Aggregator"}

    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(
                url,
                timeout=10,
                headers=headers,
                verify=False,  # SSL ignored
            )
            soup = BeautifulSoup(resp.text, "html.parser")
            paragraphs = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
            return " ".join(paragraphs[:5])
        except Exception as e:
            print(f"⚠️ Attempt {attempt} failed for {url}: {e}")
            time.sleep(2**attempt)  # exponential backoff

    print(f"⚠️ Could not fetch summary for {url} after {retries} attempts")
    return ""


def process_story(story: dict) -> dict | None:
    """Process one Hacker News story: clean fields, populate summary, detect language."""
    try:
        title = clean_text(story.get("title", ""))
        summary = clean_text(story.get("summary", ""))

        # If summary is empty, fetch from the linked article
        if not summary and story.get("url"):
            summary = fetch_summary(story["url"])

        # Skip if no title and no summary
        if not title and not summary:
            return None

        # Detect language (default unknown)
        text_for_lang = f"{title} {summary}".strip()
        language = "unknown"
        if text_for_lang:
            language = detect(text_for_lang)

        # Keep only English stories for now
        if language != "en":
            return None

        return {
            "id": story.get("id"),
            "title": title,
            "summary": summary,
            "byline": story.get("byline", ""),
            "source": story.get("source", "hackernews"),
            "publishedAt": story.get("publishedAt") or story.get("time"),
            "url": story.get("url"),
            "language": language,
        }
    except Exception as e:
        print(f"⚠️ Skipping story due to error: {e}")
        return None


def run_extraction():
    """Run extraction pipeline on ingested stories and save cleaned output."""
    # Load ingested stories
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        stories = json.load(f)

    cleaned = []
    for story in stories:
        processed = process_story(story)
        if processed:
            cleaned.append(processed)

    # Save output
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(cleaned, f, indent=2, ensure_ascii=False)

    print(f"✅ Extraction complete: {len(cleaned)} stories saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    run_extraction()
