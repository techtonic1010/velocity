# Milestone 4 — Entity Interaction Ingestion: Plan

## What this milestone is about, in plain words

Milestones 1-3 built the "what article is similar to what other article" side of the system. Milestone 4 is about the *other* half: what has each *user* actually clicked on?

We're going to build a brand-new service, `entity-interaction-service`, that:
1. Replays real click history from the MIND dataset's `behaviors.tsv` file — pretending those old, real clicks are happening live, right now.
2. Sends each click as a message onto Kafka.
3. Reads those same messages back and saves the click into two places: a permanent Postgres table (the full history, kept forever) and Redis (a small, fast "what has this user done recently" cache).

This is a brand-new Java service — nothing exists for it yet except an empty slot already reserved in `docker-compose.yml`. It's also the *first* time any Java service in this project talks to Kafka — the only Kafka code that exists so far is in Python (`vector-hasher`), so there's no existing Java pattern to copy; this plan designs that pattern for the first time.

## How this plan was put together

This wasn't just read-the-spec-and-go. Three things fed into it:
1. I read the only existing Java service in the project (`entity-upload-service`, from Milestone 1) closely, so this new service looks and feels consistent with it — same folder layout, same coding habits, same way of writing SQL.
2. I looked directly at the real `behaviors.tsv` file (156,965 real rows, 50,000 real users) instead of just trusting the written spec, since real data often has surprises the spec doesn't mention.
3. Because this milestone has more genuinely new, trickier decisions than the last one, I had a second, independent pass done on the design — which dug even deeper into the real data and caught two things my own read had missed. I then personally re-checked both of those findings myself on one real example before trusting them. Both held up:
   - **The file's row order is not the real time order.** One real user, `U13740`, appears 3 times in the file. The order the rows sit in the file is: Nov 11, then Nov 9, then Nov 13. Nov 9 happened *before* Nov 11, even though it comes *after* it in the file. So anything that needs "what happened most recently" cannot just trust file order — it has to sort by the real time value first.
   - **A user's "click history" field is a frozen snapshot, not something that grows per row.** If the same user appears in the file 3 times, that history field is *identical, word for word*, in all 3 rows. It only tells you what the user had clicked *before this whole batch of logging started* — it's not updated live.

Both of these change how the "replay" logic has to work, and are baked into the plan below.

## The two real data sources, and how each gets used

`behaviors.tsv` has, for every row: which user, when, their prior click history (a list of article IDs), and a list of articles they were shown in *this* sitting (each marked clicked or not-clicked).

- **Prior click history** (the frozen-snapshot list): treated as the user's older, background clicks. Since the raw data never tells us the *exact* time each of these individual clicks happened, we invent believable timestamps ourselves — one second apart, oldest to newest (the dataset's own documentation confirms this list is stored oldest-first), ending just before that user's earliest real logged moment. This is an honest, disclosed simplification, not a hidden one — it'll be written down plainly as a real decision made during this build, same as every other simplification in this project so far.
- **What they clicked in each sitting** (the marked-clicked articles): these get their real, actual timestamps, since the file does give a real time for each sitting.

Both sources get replayed as "this user clicked this article." We also simulate a small amount of extra reaction on top of the real clicks — pretending 10% of the "this sitting" clicks were actually a LIKE, and 5% were a DISLIKE — because MIND's real data only ever records "clicked or not," it has no actual like/dislike signal. (This simulation only ever applies to the real-timestamped clicks, never the invented-timestamp background history — it felt like a stretch to invent an emotional reaction on top of an already-invented timestamp.) Which clicks get upgraded to LIKE or DISLIKE is decided by a fixed, repeatable calculation based on the user and article together — not a random dice roll each time — so replaying the exact same user twice always produces the exact same result. That matters because a real production system replaying the same real event twice (which does happen — messaging systems occasionally deliver the same message more than once) must never change its mind about something it already decided.

## Two ways to trigger a replay

- **One user at a time** — matches this milestone's own success criterion exactly ("replay one user's real click history and confirm it shows up correctly").
- **A bounded batch of users** — replays the first N distinct real users, for testing at a slightly bigger scale, mirroring how earlier milestones always had a "do just a few" option before ever considering "do everything."

Doing "the first N *rows*" wouldn't work here, unlike earlier milestones — a single user's rows can be scattered anywhere across the file, so replaying "the first N rows" could easily replay bits and pieces of many different users' partial data instead of N complete users. The batch version has to scan through the whole file once, decide up front which N users it's going to fully replay, and only keep rows belonging to those N users as it goes.

## Where the data ends up

**Postgres — a new "Entity History" table.** This is the permanent, durable record: which user clicked which article, when, and whether it was a plain click, a like, or a dislike. Since the same real click could theoretically be replayed twice (if a message gets redelivered, or if someone re-runs a replay by mistake), inserting the same record twice must not create a duplicate row — it should just update the existing one in place. To make that work reliably, each event carries a small extra field identifying exactly *which* real record it came from (the real MIND impression ID for "this sitting" clicks, or a simple made-up marker for background-history clicks) — this is a small, deliberate addition beyond the plainest version of the design, and it'll be logged as such.

**Redis — three small pieces of fast, per-user memory:**
- **Last 5 things clicked** — a short list, most-recent-first. Every new click pushes onto the front, and if the article's already somewhere in the list, it moves to the front instead of appearing twice.
- **A Bloom filter** — a well-known, compact way to answer "has this user probably already seen this article?" using a small fixed amount of memory, without needing to store every single article ID they've ever touched. It can occasionally say "maybe seen" when it wasn't really (a false alarm), but it can never wrongly say "definitely not seen" when it actually was — that's the whole point of this data structure, and it's why Milestone 5 can safely use it as a fast first check before ever bothering to ask Postgres. This project builds this filter's actual math by hand (rather than reaching for a ready-made Redis add-on), consistent with how every other core algorithm in this project (the similarity hashing, the neighbor heap) has been hand-built rather than delegated to a library. It's sized based on real data — most real users in this dataset have touched somewhere around a few dozen articles, so it's sized generously enough to comfortably cover the large majority of real users at a low false-alarm rate.
- **Likes/dislikes** — a small lookup of "this user felt this way about this specific article," only ever populated for the simulated LIKE/DISLIKE events, never plain clicks.

## Rough shape of the new service

Following the same folder pattern as the existing Java service (one folder per responsibility — a place for web endpoints, a place for business logic, a place for database code, and so on):

```
entity-interaction-service/
├── Dockerfile                — same build recipe style as the existing Java service
├── pom.xml                   — adds Kafka support and Redis support to the usual setup
└── src/main/java/.../entityinteraction/
    ├── controller/           — the two replay endpoints + a health check
    ├── listener/             — the Kafka "listen and react" code
    ├── client/               — the Kafka "send a message" code
    ├── service/               — the actual replay logic, and the "what to do when a message arrives" logic
    ├── repository/            — talks to Postgres (history table) and Redis (last-5 / bloom filter / likes)
    ├── config/                — Kafka setup/wiring
    ├── dto/ , model/          — plain data shapes (an interaction event, a parsed row from the file, etc.)
    ├── parser/                — turns raw behaviors.tsv lines into usable data
    └── util/                  — the shared hashing math (used by the Bloom filter, the like/dislike
                                 decision, and deciding which "shard" a user's data logically belongs to)
```

## New Postgres table

```sql
CREATE TABLE IF NOT EXISTS entity_history (
    source_id         VARCHAR(64)  NOT NULL,   -- identifies exactly which real record this came from
    entity_id         VARCHAR(32)  NOT NULL,
    user_id           VARCHAR(32)  NOT NULL,
    interaction_type  VARCHAR(10)  NOT NULL,   -- CLICK, LIKE, or DISLIKE
    event_timestamp   TIMESTAMPTZ  NOT NULL,
    shard_id          INTEGER      NOT NULL,   -- logical grouping by user, matching this project's
                                                -- existing habit of a shard-id column rather than
                                                -- physically separate databases
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (source_id, entity_id)
);
```
Plus lookups by article ID and by time, since both are needed later. This file gets added the same way the last two milestones' schema files were — auto-applied to a brand-new database, but since this project's database has already been running a while, it'll need to be applied by hand too, exactly like Milestone 3's schema addition was.

## Build order (same one-piece-at-a-time habit as every milestone so far)

1. Add the new database table, prove by hand that inserting the same record twice updates rather than duplicates.
2. Get a bare-bones version of the service running (just a health check), talking to the real Postgres, Kafka, and Redis containers.
3. Write the code that reads real rows out of `behaviors.tsv` correctly — proven against one specific real user's rows.
4. Build the "send to Kafka" side and the single-user replay endpoint — proven by watching the real Kafka messages come out in the right order for that same real user.
5. Build the "receive from Kafka, save to Postgres" side — proven by checking the real database afterward, and proving that replaying the same user twice doesn't create duplicate rows.
6. Build the Redis side (last-5 list, Bloom filter, likes/dislikes) — proven against hand-calculated expected values for that same real user.
7. Build the batch-replay endpoint, and try it at a slightly bigger scale to get a real sense of how fast it runs.
8. Write down every simplification made along the way in this project's running "defensibility" log, same habit as every milestone before this one.

## What's next

This document is the plan only — nothing has been built yet. When you're ready to actually build this milestone, we'll go through it file by file, the same way Milestones 1-3 were built.
