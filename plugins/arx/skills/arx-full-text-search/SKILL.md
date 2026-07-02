---
name: arx-full-text-search
description: Search cached arx-indexed arXiv paper text with BM25 snippets, scopes, and optional per-paper filters.
---

# arx-full-text-search

Use this skill when a user asks to find claims, methods, citations, definitions, equations, related work, or other text inside papers already cached by arx.

## Workflow

1. Use `lookup_arxiv_papers` first when the user names a specific paper and you do not know whether it is cached.
2. Call `full_text_search` with concise keyword-style queries. It searches cached local material only and never contacts arXiv.
3. Use `arxiv_id` when the user wants results from one paper. Omit it when the user wants to search across the local arx index.
4. Choose the narrowest useful scope:
   - `default` or omitted searches title, metadata, and body text, excluding bibliography.
   - `titles` searches paper titles only.
   - `bibliography` searches citation records and `.bib` or `.bbl` material only.
   - `all` searches everything.
5. Read the returned snippets, scores, source paths, and line ranges. Use them as grounded evidence and cite the paper ids or file locations in your answer when helpful.

## Notes

- Empty results may include a `note` that explains whether the paper is uncached, metadata-only, or missing bibliography/body material.
- If the paper is missing source or body text, use `arx-paper-fetch` to queue the needed material before retrying.
- Prefer a few targeted searches over one broad natural-language question.
