# Lab 12 – AI Capstone

Part A: RAG on Databricks over EIA petroleum documents.

## Dataset

Public documents from the U.S. Energy Information Administration (EIA), same topic as the petroleum pipeline.

| Source | Type | Documents |
|---|---|---|
| Energy Explained: oil, gasoline, diesel fuel, heating oil, hydrocarbon gas liquids | long structured pages | 46 (5 section pages + 41 subpages) |
| EIA FAQs | short answers | 33 |

Why this dataset:
- Same domain as the rest of the project.
- Two document types: long pages (split by headings) and short FAQs (one chunk each).
- Enough documents to write 50+ evaluation questions with known source documents.

Selection rules:
- Energy Explained: only petroleum-related sections.
- FAQs: only questions mostly about crude oil, gasoline, diesel and heating oil. General multi-fuel questions (electricity, coal, state data) were excluded after manual review.
- A FAQ can belong to several EIA categories, so categories are stored as a list.

## Notes

- Raw HTML is stored first, parsing and chunking are separate steps (keeps the source for audit and re-parsing).
- Pages are downloaded once, with a 1 second pause between requests.
- The table is fully replaced on each run (small corpus).
- EIA pages have no publication date, so `retrieved_at` is used instead.
- Some FAQ answers mostly link to other pages, so evaluation questions are written mainly from the long pages.
