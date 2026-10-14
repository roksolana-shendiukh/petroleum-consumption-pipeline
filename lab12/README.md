# Lab 12 – AI Capstone

Part A: RAG on Databricks over EIA petroleum documents.

## Dataset

Public knowledge-base articles from the U.S. Energy Information Administration (EIA), the same domain as the petroleum pipeline. **79 documents** in total.

| Source | Type | Documents |
|---|---|---|
| Energy Explained: oil, gasoline, diesel fuel, heating oil, hydrocarbon gas liquids | long structured pages | 46 (5 section pages + 41 subpages) |
| EIA FAQs | short answers | 33 |

**Selection rules**
- Energy Explained: petroleum-related sections only.
- FAQs: only questions mostly about crude oil, gasoline, diesel and heating oil; general multi-fuel questions (electricity, coal, state data) were excluded after manual review.
- A FAQ can belong to several EIA categories, so categories are stored as a list.

**Notes**
- Raw HTML is stored first; parsing and chunking are separate steps (the source stays available for audit and re-parsing).
- Pages are downloaded once, with a 1 second pause between requests.
- Each run replaces the whole table (the corpus is small); incremental MERGE by content_hash is a possible next step.
- Some FAQ answers mostly link to other pages, so evaluation questions are written mainly from the long pages.

<details>
<summary>Verification: loaded documents by type</summary>

<img src="screenshots/01_raw_documents.png" alt="raw_documents by doc_type" width="700">

</details>

## HTML parsing (clean text)

Rules are configuration, not code: per source, the content container, title selectors, tags to drop and noise lines to skip (`config/parse_documents.json`).

| What we do | Why |
|---|---|
| Read only the main article container; drop configured tags and comments | navigation and scripts would pollute embeddings |
| Flatten tables to text lines (caption + rows) | tables hold the numbers questions ask about, and plain text embeds well |
| Unwrap inline tags (links, bold) and keep list bullets | a sentence stays on one line instead of fragments |
| Split into sections at headings | one section = one topic, so chunks are focused and citations are clear |
| Move "Last updated" to the `last_updated` field | keeps the text clean and allows freshness checks |
| Keep selectors and noise lines in config | a site layout change is fixed by editing config, not code |

Not captured: charts and photos (images). A few photo captions and "Did you know?" blocks remain as noise.

<details>
<summary>Verification: parsed sections of one page</summary>

<img src="screenshots/02_parsed_sections.png" alt="parsed_sections sample" width="700">

</details>
