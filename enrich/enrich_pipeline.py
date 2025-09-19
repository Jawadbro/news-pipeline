import json
import os
from collections import defaultdict
from langdetect import detect
from transformers import pipeline
import spacy

# Load NER model
nlp_ner = spacy.load("en_core_web_sm")  # lightweight English NER

# Zero-shot classifier for topics and content type
classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")

# Files
INPUT_FILE = os.path.join("data", "extracted", "hn_clean.json")
OUTPUT_FILE = os.path.join("data", "enriched", "hn_enriched.json")
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

# Candidate labels
TOPIC_LABELS = [
    "technology",
    "science",
    "health",
    "finance",
    "politics",
    "design",
    "culture",
    "education",
    "sports",
]
CONTENT_TYPE_LABELS = ["news", "opinion", "press release", "tutorial", "blog"]

# Optional: source reputation
SOURCE_REPUTATION = {"HackerNews": 0.8, "medium": 0.6, "wikipedia": 1.0}


def extract_entities(text: str) -> list:
    """Named Entity Recognition using spaCy"""
    doc = nlp_ner(text)
    entities = []
    for ent in doc.ents:
        entities.append(
            {
                "text": ent.text,
                "label": ent.label_,
                "kb_id": None,  # placeholder: could link to Wikidata/Wikipedia
            }
        )
    return entities


def classify_text(text: str, candidate_labels: list) -> list:
    """Zero-shot classification (multi-label)"""
    if not text.strip():
        return []
    result = classifier(text, candidate_labels)
    return list(zip(result["labels"], result["scores"]))


def enrich_story(story: dict) -> dict:
    text = f"{story.get('title', '')} {story.get('summary', '')}"

    entities = extract_entities(text)
    topics = classify_text(text, TOPIC_LABELS)
    content_type = classify_text(text, CONTENT_TYPE_LABELS)

    return {
        **story,
        "entities": entities,
        "topics": [{"label": l, "score": s} for l, s in topics],
        "content_type": [{"label": l, "score": s} for l, s in content_type],
        "word_count": len(text.split()),
        "source_score": SOURCE_REPUTATION.get(story.get("source"), 0.5),
    }


def run_enrichment():
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        stories = json.load(f)

    enriched = []
    for story in stories:
        enriched.append(enrich_story(story))

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(enriched, f, indent=2, ensure_ascii=False)

    print(f"✅ Enrichment complete: {len(enriched)} stories saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    run_enrichment()
