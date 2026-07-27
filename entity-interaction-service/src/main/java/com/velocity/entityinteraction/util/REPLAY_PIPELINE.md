# Replay pipeline — what each step actually does

## The raw dataset (`behaviors.tsv`)

Tab-separated, no header, 5 columns:

```
ImpressionID   UserID   Time                    History              Impressions
1              U13740   11/11/2019 9:05:58 AM   N55189 N42782 ...    N55689-1 N35729-0
```

- **History** — space-separated article IDs the user clicked *before* this file's log period started. Same for every row belonging to one user (it's frozen, not per-row).
- **Impressions** — space-separated `articleId-label` pairs *for this one row*. `1` = clicked, `0` = shown but not clicked.

That's it. Nothing else in the raw file is timestamped, typed, or classified — everything below is computed.

## Step 1 — `BehaviorsTsvParser`: text line → `BehaviorRow`

Turns one raw line into a typed object. What it adds that wasn't explicit in the file:
- `split` — `"TRAIN"` or `"DEV"`, based on which file was read (not a column in the file itself).
- `time` — the raw `"11/11/2019 9:05:58 AM"` string parsed into a real `LocalDateTime`.
- `historyEntityIds` — the History string split into a list.
- `impressions` — the Impressions string split into a list of `(entityId, clicked)` pairs.

## Step 2 — `UserTimelineBuilder`: a user's `BehaviorRow`s → `InteractionEvent`s

This is where all the real computation happens. Input: every row belonging to one user (both files combined). Output: one flat, time-ordered list of events.

**2a. Sort.** Rows are sorted by `time` — the file order is not trustworthy (a user's rows are not guaranteed to appear chronologically).

**2b. Turn History into fake "past" events.** History has no timestamps or IDs of its own, so this step invents both:
- `sourceId` — computed as `"HIST-{userId}-{index}"`, where `index` is the item's position in the list. This is what makes a genuinely repeated article in someone's History (it happens) end up as two separate rows later, instead of one overwriting the other.
- `timestamp` — computed by counting backward from the user's earliest row: 1 second per item, so the whole History list lands in the second(s) just before their first real activity.
- `interactionType` — always `CLICK` (History never gets LIKE/DISLIKE).

**2c. Turn each real click (label=1) into an event.**
- `sourceId` — computed as `"{split}-{impressionId}"` (e.g. `"TRAIN-8821"`). The split prefix exists because `impressionId` numbers restart from 1 in both files — without the prefix, a train row and a dev row could collide.
- `timestamp` — just the row's real `time`, unchanged.
- `interactionType` — **computed**, not given. See step 3.

## Step 3 — the LIKE/DISLIKE computation (inside `UserTimelineBuilder.classify`)

The raw data only has click/no-click — LIKE and DISLIKE don't exist in MIND. This step manufactures them deterministically so the same (user, article) pair always gets the same label, even if it's genuinely clicked twice in two different real sessions:

1. Build the string `"{userId}|{entityId}"`.
2. Hash it with `MurmurHash3.hash32(...)` → a 32-bit number.
3. Convert that number to a fraction between 0 and 1 (`hash / 2^32`).
4. `< 0.05` → `DISLIKE`, `< 0.15` → `LIKE`, otherwise → `CLICK`.

## `MurmurHash3`

Just a hash function: bytes in, one deterministic 32-bit number out. Used above for LIKE/DISLIKE, and reused later (with a second seed) for the Bloom filter's bit positions — one shared hashing primitive, two different consumers.

## End to end

```
raw line  →  BehaviorRow        (typed, split-tagged, time-parsed)
          →  sorted by time
          →  History items      → synthetic CLICK events, HIST-{userId}-{index}
          →  real clicked items → CLICK/LIKE/DISLIKE events, {split}-{impressionId}
          →  InteractionEvent list, in chronological order
          →  published to Kafka, one at a time, in that order
```
