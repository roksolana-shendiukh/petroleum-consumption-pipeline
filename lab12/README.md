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

**Verification:** loaded documents by type

<img src="screenshots/01_raw_documents.png" alt="raw_documents by doc_type" width="700">


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

**Verification:** parsed sections of one page

<img src="screenshots/02_parsed_sections.png" alt="parsed_sections sample" width="700">


## Chunking

| What done | Why |
|---|---|
| Recursive splitter: line -> sentence -> word -> character | cuts at natural boundaries before cutting mid-sentence |
| Max 1600 characters (~400 tokens) | fits the embedding model input and keeps one topic per chunk |
| Overlap 240 characters (~15%) | a fact on a chunk border appears whole in at least one chunk |
| Merge sections shorter than 400 characters with a neighbour (up to 1600) | tiny chunks match many queries and carry little meaning |
| Prepend `Title` and `Section` to every chunk | a chunk is understandable on its own, and the title becomes part of the embedding |
| Never cross document borders; FAQ answers stay one chunk each | answers are short and self-contained; sources stay clean |
| Stable ids `doc_id#0000` and metadata copied from the document | citations, gold labels in evaluation and metadata filters |
| Token count estimated as characters / 4 | cheap approximation without a tokenizer |

Known issue: the overlap can start a chunk in the middle of a list, so the list's intro sentence stays in the previous chunk (for example `imports-and-exports#0003`: crude-oil import shares without their label).

**Verification:** chunk statistics and a sample chunk

<img src="screenshots/03_chunks_stats.png" width="700">

<img src="screenshots/04_chunks_sample.png" width="700">

## Embeddings and governed storage

| What is done | Why |
|---|---|
| Every chunk is embedded with `databricks-gte-large-en` (1024 dimensions) via `ai_query` in Spark | One managed model for chunks and queries, so vectors are comparable. No model hosting is needed. |
| Vectors are stored in the Unity Catalog Delta table `chunk_embeddings` (191 rows), next to `chunk_id`, `doc_id`, `title`, `section`, `source_url` and `chunk_text` | The table is governed by Unity Catalog (permissions, lineage). Every vector can be traced back to its source page. |
| `embedding_model` and `embedded_at` are saved in each row | It is always known which model made a vector. Changing the model requires re-embedding, and the columns show which rows are stale. |
| Vectors are kept in a table, not only inside the search index | The index can be deleted and rebuilt from the table without paying for embeddings again. |
| The table is created explicitly from `config/tables.json` in the `lab12-setup` job | No hidden infrastructure. The schema is versioned in git. |
| Change Data Feed is enabled on the table | It is required by the Delta Sync index to pick up changes. |
| Dimensions are checked after embedding | A wrong or empty vector would break the index later. |


**Verification:** 191 chunks, all vectors have 1024 dimensions
<img src="screenshots/05_embeddings.png" width="700">

