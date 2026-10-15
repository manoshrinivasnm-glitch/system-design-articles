# Article guide

Every problem is one Markdown file, `p/<slug>.md`, where the slug comes from `_data/problems.json`.
GitHub Pages renders it at `/p/<slug>.html`, and github.com renders the same file directly (diagrams included).

The goal of each article: **you can read it in 15 minutes and talk through it for 30.**
It is a script for the interview, not a textbook. Short sentences, bullets, tables. No history lessons, no marketing.

## Front matter

```
---
title: "Design a URL Shortener"           # from problems.json
id: 1
slug: 01-url-shortener
aka: "TinyURL, Bitly"
category: "Core building blocks"
difficulty: Easy
---
```

`tools/build_site.py` adds `prev_*` / `next_*` fields. Do not write them by hand.

## Sections, in this order, with these exact headings

| Heading | What goes in it | Size |
|---|---|---|
| (no heading) one blockquote line | `> **In one line:** ...` The core idea of the final design in one sentence. | 1 line |
| `## The design at a glance` | The **final** architecture as a Mermaid flowchart, then 2-3 sentences that walk the main path through it. This is "the picture" you draw on the whiteboard. | 1 diagram |
| `## How to spend the 30 minutes` | Table: minutes, what you do, what to say or draw. Adjust the split to the problem. | 6-8 rows |
| `## Requirements` | `**Functional**`, `**Non-functional**` (with numbers: latency, availability, consistency needs), `**Out of scope**`, each a short bullet list. End with a `> **Say it like this:** "..."` line you would actually say to the interviewer. | 12-20 bullets |
| `## Back-of-the-envelope numbers` | Table: quantity, estimate, how you got it. Then `**What the numbers tell us:**` with 2-4 bullets that turn numbers into design decisions (read-heavy so cache, 50 TB so object storage, etc.). | 6-10 rows |
| `## API` | A ` ```http ` block with the 3-6 core endpoints (method, path, key fields). One bullet per non-obvious choice (idempotency key, pagination cursor, WebSocket vs polling). | |
| `## Data model` | The main entities as a table or a ` ```text ` block, then which database holds each and **why** (one line each). Mention the partition/shard key. | |
| `## Design v1: the simplest thing that works` | A small Mermaid diagram (4-7 nodes), then numbered steps for one request end to end. This is what a junior engineer would draw first, and it should honestly work at small scale. | 1 diagram |
| `## Where v1 breaks` | Table: problem, why it happens, what the user sees. 4-6 rows. These are the flaws the interviewer wants you to find yourself. | |
| `## Design v2: <short name of the big idea>` | For each fix: `### Fix N: <name>` with 2-5 lines (what changes, why it solves the row above). If v2 is the final design, say "This is the diagram at the top" and add a `### Components` table (component, job, tech choice). | |
| `## Where v2 still breaks` + `## Design v3: <name>` | **Optional.** Use when the problem has a natural third step (flash sales, celebrities, hot shards, multi-region). The last version must match "The design at a glance". | |
| `## Key flow: <name>` | A Mermaid `sequenceDiagram` (use `autonumber`) of the single most important flow (booking a seat, sending a message, charging a card), then a few bullets on the failure cases inside that flow. | 1 diagram |
| `## Deep dives` | 3-5 `### <topic>` subsections: the questions interviewers love for this problem. Each: **The problem.** one or two lines, **Options.** a table (option, good, bad), **Pick.** what you choose and why. | |
| `## Trade-offs to say out loud` | 4-6 bullets in the form "We chose X over Y because ... The cost is ...". | |
| `## Follow-up questions` | 6-8 likely follow-ups, each as `**Q: ...**` followed by a 1-3 sentence answer. | |
| `## 60-second recap` | 5-7 bullets. If the interviewer said "summarize your design", this is what you say. | |

Target length: **2,500-3,500 words** per article, diagrams not counted. Shorter is fine if nothing is missing.

## Diagrams (Mermaid)

Diagrams are fenced ` ```mermaid ` blocks. github.com renders them natively; the Pages site renders them with `assets/article.js`.
Each article has at least three: the final architecture, the v1 architecture, and the key-flow sequence diagram. A v2 or v3 diagram is welcome when it differs from the final one.

Shapes, so every article reads the same way:

| Thing | Mermaid | Example |
|---|---|---|
| Client / user | stadium | `U(["Mobile / web app"])` |
| Service, gateway, worker | rectangle | `API["API gateway"]` |
| Database, cache, object store | cylinder | `DB[("Postgres<br/>bookings")]` |
| Queue / stream / topic | subroutine | `Q[["Kafka<br/>payment-events"]]` |
| Third-party / external system | flag | `PSP>"Payment provider"]` |
| Group (region, tier, pipeline) | subgraph | `subgraph Write path` ... `end` |

Rules that keep diagrams from breaking:

- Put every label in double quotes: `A["Seat service"]`, `A -->|"hold seat"| B`. Parentheses, slashes, colons and commas in unquoted labels break the parser.
- Line breaks inside labels: `<br/>`. No `;` or `#` anywhere inside a diagram, no double quotes inside a label.
- Never write `{{`, `}}` or `{%` anywhere in the file, including diagrams (Jekyll's Liquid would eat them). So no hexagon shapes.
- Node ids are short and plain (`API`, `SeatSvc`, `DB1`). Never use `end`, `graph`, `subgraph` or `style` as an id.
- Solid arrow `-->` for synchronous calls, dotted `-.->` for async events or background jobs.
- Stay readable: at most about 16 nodes in a flowchart. Use `flowchart LR` for pipelines, `flowchart TB` for layered systems.
- No colours or `classDef` (they clash with dark mode). Use `subgraph` titles to highlight what is new in a version.
- Only classic Mermaid syntax (no `@{ shape: ... }`), so it renders on both github.com and the site.
- Check every file: `node tools/check_diagrams.mjs p/<slug>.md` (needs `npm install` once). It parses every diagram and fails on Liquid braces.

## Writing style

- Talk to the reader as "you". Write the way you would explain it to a friend across the table.
- Prefer a table or a list over a paragraph. Paragraphs are at most 3 sentences.
- Every technology choice gets a reason ("Redis, because the hold must expire on its own after 10 minutes").
- Use real, round numbers. Show the arithmetic in the estimates table.
- Name the flaw before the fix. The interviewer is grading whether you see problems coming.
- Links to other articles use relative Markdown links, for example `[rate limiter](21-rate-limiter.md)`. These work on github.com and on the site.
- All text and diagrams must be original. Do not copy text or images from courses, blogs or videos.

Run `python3 tools/build_site.py` after adding or renaming an article. It rebuilds `index.md` and the prev/next links.
