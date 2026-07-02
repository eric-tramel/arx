---
name: arx-paper-fetch
description: Add arXiv papers to arx by queueing downloads through arxd and tracking the durable download queue.
---

# arx-paper-fetch

Use this skill when a user wants a paper added to the local arx cache, needs PDF or TeX source material, or needs to know how arx downloading works.

## Workflow

1. Start with `lookup_arxiv_papers` for the target ids. Confirm which material is already cached and which material is missing.
2. Call `fetch_arxiv_paper` only for missing material that is actually needed. It queues work through `arxd` and returns immediately with a job id.
3. Tell the user that queued work runs in the background. Do not assume the PDF or source is ready until the queue says so.
4. Call `get_arxiv_download_queue_status` with the returned `job_id` to inspect one job, or with `include_finished: true` to see completed and failed jobs after `arxd` exits.
5. After a completed download, use `lookup_arxiv_papers` again to refresh readiness, then use `full_text_search` when the task requires paper text.

## Notes

- `arxd` enforces the shared arXiv request delay across CLI and MCP clients.
- Finished job records are durable across `arxd` restarts, so `include_finished: true` is the reliable default for follow-up checks.
- Do not queue downloads when metadata is enough for the user's task.
- If a job fails, report the failure reason from the queue status and avoid retry loops unless the user asks.
