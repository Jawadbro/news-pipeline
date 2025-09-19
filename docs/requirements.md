# Requirements - Step 0

## Scope (v1)
- Collect news from RSS/Atom/JSON feeds
- Normalize, clean, enrich with topics/entities
- Deduplicate, cluster, rank
- Summarize headlines
- Provide API for HUD

## Non-Goals (v1)
- No video/audio ingestion
- No paywalled sources
- No deep personalization yet

## Latency Budget
- Ingest → storage: < 5s
- Ranker API P95: < 200ms
- Summarization: < 500ms

## Metrics
- Ingest success rate > 95%
- Deduplication F1 > 0.8
- Precision@10 ≥ baseline
