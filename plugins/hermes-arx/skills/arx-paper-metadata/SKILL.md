---
name: arx-paper-metadata
description: Review arXiv paper metadata, abstracts, local readiness, and cache paths with arx before deciding whether to fetch full paper material.
---

# arx-paper-metadata

Use this skill when a user asks about one or more arXiv papers and you need the paper identity, abstract, metadata, local cache state, or the best next arx action.

## Workflow

1. Normalize the user-provided arXiv ids before calling tools. Preserve version suffixes only when the user explicitly asks for a specific version.
2. Call `lookup_arxiv_papers` first. It returns metadata, abstract text, local material readiness, cache paths, and per-paper errors.
3. Treat the lookup result as the source of truth for whether metadata, PDF, source, and index material are already present locally.
4. If the user asked for a no-network check, pass `fetch_missing_metadata: false`. Otherwise, `lookup_arxiv_papers` may fetch missing metadata but never downloads PDFs or source archives.
5. Use the returned `next_tool` or readiness fields to decide whether to search cached text or queue a download.

## Notes

- Batch related paper ids in one `lookup_arxiv_papers` call.
- Cached metadata should still be used when a network refresh fails.
- Do not call `fetch_arxiv_paper` just to inspect metadata.
- Report per-paper lookup errors separately instead of treating a partial batch failure as a total failure.
