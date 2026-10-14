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

| What is done | Why |
|---|---|
| Read only the main article container; drop configured tags and comments | navigation and scripts would pollute embeddings |
| Flatten tables to text lines (caption + rows) | tables hold the numbers ask about, and plain text embeds well |
| Unwrap inline tags (links, bold) and keep list bullets | a sentence stays on one line instead of fragments |
| Split into sections at headings | one section = one topic, so chunks are focused and citations are clear |
| Move "Last updated" to the `last_updated` field | keeps the text clean and allows freshness checks |
| Keep selectors and noise lines in config | a site layout change is fixed by editing config, not code |

Not captured: charts and photos (images). A few photo captions and "Did you know?" blocks remain as noise.

**Verification:** parsed sections of one page

<img src="screenshots/02_parsed_sections.png" alt="parsed_sections sample" width="700">


## Chunking

| What is done | Why |
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


## Search index and retrieval

| What is done | Why |
|---|---|
| A Delta Sync index `chunk_embeddings_index` is built on top of the `chunk_embeddings` table | The index follows the table. After a table change, one sync updates it, and no separate copy of the data is maintained by hand. |
| Embeddings are self-managed: the index uses the stored `embedding` column (1024 dimensions) | Vectors are computed once in Spark and reused. Index rebuild costs no extra embedding calls. |
| Queries are embedded with the same `databricks-gte-large-en` model | Query and chunk vectors must come from the same model, otherwise similarity scores are meaningless. |
| `chunk_id` is the primary key, and `doc_id`, `title`, `part_heading`, `doc_type`, `section`, `categories`, `source_url`, `last_updated` and `chunk_text` are synced into the index | Metadata makes filtering possible and lets every answer cite its source. `chunk_text` is returned directly, so no second lookup is needed. |
| Sync mode is `TRIGGERED` | The corpus changes rarely. Sync runs only on demand, which is cheaper than continuous sync. |
| A `STANDARD` endpoint `lab12_search_endpoint` hosts the index | The only endpoint type available in the workspace. One endpoint serves all queries. |
| The endpoint and the index are created and deleted by separate jobs (`lab12-search-create`, `lab12-search-delete`) | Endpoints are billed while they exist. Creation is explicit and cleanup is one click. |
| Retrieval returns the top 5 chunks (`top_k`) | Enough context for most questions, short enough to keep the prompt small and cheap. |
| Both ANN (vector) and HYBRID (vector + keyword) search are supported | Pure vector search can miss exact terms such as product names, years and numbers. Hybrid search covers them. |
| Metadata filters (for example by `doc_type` or `doc_id`) can be passed with a query | Search can be limited to a subset of documents without changing the index. |
| Retrieval is evaluated separately from generation (`lab12-retrieval-eval`, k = 10, ANN vs HYBRID) | If the answer is wrong, it is known whether retrieval or the LLM is the cause. |

**Verification:** 

<img src="screenshots/06_search_index.png" width="700">

## Question answering workflow

| What is done | Why |
|---|---|
| For every question the pipeline runs retrieve, build prompt, call LLM, store answer (`lab12-generate`) | The whole path from question to answer is one repeatable job, not manual steps. |
| Retrieval uses HYBRID search with the top 5 chunks | Keyword matching helps with numbers, years and product names, which pure vector search can miss. |
| Retrieved chunks are put into the prompt with their title and section, and the system prompt says to answer only from this context | Answers can be traced to a source, and the model is pushed away from using its own memory. |
| A fixed refusal phrase is defined: "I cannot answer this question from the provided documents." | Refusals are detected by exact match, so unanswerable can be measured without guessing. |
| Llama 3.3 70B (`databricks-meta-llama-3-3-70b-instruct`) is called with temperature 0 and max 600 tokens | Deterministic output makes runs comparable, and a token limit keeps cost and latency bounded. |
| Every question is answered in two modes: `no_retrieval` (question only) and `retrieval` (question and chunks) | The same model and give a direct with/without retrieval comparison. |
| The model is called through the Databricks SDK with retries and a pause between attempts | A temporary endpoint error does not lose a row. Failures stay visible in the `error` column. |
| Each result is appended to the Delta table `generation_results` with `run_id`, `mode`, `prompt`, `context_chunk_ids`, `answer`, `latency_ms` and `error` | Every answer can be reproduced and audited: exactly which prompt and which chunks produced it. |
| Questions come from the train and dev splits (45 of 50 questions) | The test split (5 questions) stays untouched, so it can be used once for the final evaluation. |

**Verification:** the same question answered without and with retrieval (with the chunk ids used as context), and the number of questions per split

<img src="screenshots/07_generation_results.png" width="700">

<img src="screenshots/08_eval_splits.png" width="700">

## Evaluation

| What is done | Why |
|---|---|
| Every answer from both modes is scored by a separate judge model (`databricks-gpt-oss-120b`) | The judge is a different model from the generator, which reduces the bias of a model grading its own style. |
| Three scores from 1 to 5: correctness (against the reference answer), relevance (to the question), groundedness (supported by the retrieved chunks) | They cover the three quality aspects: right content, on-topic answer, and hallucination risk. |
| Groundedness is scored only in `retrieval` mode | Without retrieval there is no context to check against, so the value is empty. |
| The judge also reports whether the model refused to answer, and `behavior_correct` compares it with whether the question is answerable | Refusing an unanswerable question is correct, answering it is a hallucination, and refusing an answerable one points to missing context. |
| `behavior_correct` is left empty for adversarial questions | Their expected behavior is not always a refusal, so counting them would distort the rate. |
| Results are stored per answer in `judge_results` with the judge's reasoning | Each score can be inspected and challenged. |
| A sample of answers was checked by hand | Correctness agreed with the judge in 6 of 6 cases, so the scores are trustworthy for a first assessment. |

**Results:** average scores from 1 to 5 on 45 questions (`train` + `dev`), by mode and by question type, and the counts behind hallucination risk and missing context

<img src="screenshots/09_judge_by_mode.png" width="700">

<img src="screenshots/10_judge_by_type.png" width="700">

<img src="screenshots/11_judge_risks.png" width="700">

Findings:

| Aspect | Result |
|---|---|
| Relevance | Equal in both modes (4.84). The model stays on topic with or without context. |
| Correctness | Retrieval adds +0.93 overall (3.76 -> 4.69), most of all for factual questions (3.58 -> 4.92), where exact figures matter. |
| Hallucination risk | Without retrieval, 1 unanswerable question got an answer. With retrieval, 0. |
| Missing context | 1 answerable question was refused in each mode, which shows a retrieval gap. |
| Weak spots | Adversarial questions get slightly worse with retrieval (3.67, groundedness 2.67), and 2 answers have low groundedness. Multi-document questions do not improve, and their relevance drops from 5 to 4. |


## Logging, monitoring and access

| What is done | Why |
|---|---|
| Every prompt, the retrieved chunk ids, the answer, latency and error are stored per question in `generation_results` | Any answer can be reproduced and audited: it is known exactly what the model saw. |
| Judge scores are stored in `judge_results` with `generation_run_id` | Each score is linked to the answer it evaluates. |
| Result tables are append-only and every row has a `run_id` | History is kept, and runs can be compared. |
| Each question is traced with MLflow in the experiment `lab12_rag_monitoring`, with spans for the whole call, retrieval and generation, and a `run_id` tag | The trace shows where time is spent and where an error happened, and links back to the run. |
| Traces are stored in Unity Catalog tables (`rag__otel_*`) | Traces are governed and queryable with SQL like any other data. |
| Built-in scorers are registered on the experiment: `safety` (100% of traces), `relevance_to_query` (50%), `retrieval_groundedness` (50%) | Quality is checked automatically without manual judge runs, and sampling keeps the cost low. |
| Access is controlled by Unity Catalog permissions: read on source tables, write on result tables, query on model endpoints, and write on the trace tables for the monitoring identity | Least privilege: each part of the pipeline gets only the access it needs. |

Observation: `relevance_to_query` often marks a refusal as `Fail`. For unanswerable questions the refusal is the correct behavior, so these `Fail` results are expected and should be read together with the question type.

**Verification:** usage overview of the experiment, traces with scorer results, spans of one trace, and the safety assessment with its rationale

<img src="screenshots/12_monitoring_overview.png" width="700">

<img src="screenshots/13_traces_scorers.png" width="700">

<img src="screenshots/14_trace_spans.png" width="700">

<img src="screenshots/15_trace_assessment.png" width="700">

## Limitations

| Limitation | Impact |
|---|---|
| Small evaluation set: 50 questions, only 5 in `test` | Metrics are rough, one question in `test` is 20% of the score. |
| The judge is an LLM, and agreement with a human was checked only on a sample | Scores are indicative, not final. |
| Adversarial and multi-document questions do not improve with retrieval | Their causes are not investigated yet. |
| Overlap can start a chunk in the middle of a list | A few chunks have incomplete context. |
| The `relevance_to_query` scorer marks correct refusals as `Fail` | Results must be read together with the question type. |
| Free Edition: one user, one search endpoint, serverless compute only | Real roles and least-privilege grants are not configured, the access model is only described. |
| Production monitoring is in Beta | Behavior and interface may change. |

## Discussion

| Topic | Conclusion |
|---|---|
| RAG vs prompting | Retrieval raises correctness from 3.76 to 4.69 (factual: 3.58 to 4.92) and removes the one answer given to an unanswerable question. Every answer also gets a source. The price is an index and a longer prompt. |
| Chunk size vs quality | Chunks are up to 1600 characters with 15% overlap and a title header. Smaller chunks match more precisely but lose context, larger ones keep context but blur the embedding and cost more tokens. Only one size was evaluated, so a measured comparison is a next step. |
| Governance | Data, traces and scores are in Unity Catalog, and every answer is linked to its `run_id`, prompt and chunk ids. Access follows least privilege, and no secrets are stored in code. |
| Cost | Embeddings are computed once and stored. The endpoint exists only while needed and is created and deleted by explicit jobs. Scorers are sampled, and the LLM runs at temperature 0 with a token limit. |
| Endpoint creation via Python SDK | Unlike UI clicks, the code is reviewed in git and repeatable. Unlike raw REST calls, authentication and errors are handled by the SDK. It also fits CI/CD in the next part. |
| Throughput | A question takes 1.48 s on average, so one sequential worker gives about 0.7 queries per second. Retrieval takes about 0.17 s, so the LLM call is the bottleneck. Limits of parallel load were not measured. |
