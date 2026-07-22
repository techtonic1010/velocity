from collections import OrderedDict
from re import split
from typing import Any


class LRUCache:
    def __init__(self, max_size: int) -> None:
        self.max_size = max_size
        self._store: OrderedDict[str, Any] = OrderedDict()

    def get(self, key: str) -> Any | None:
        if key not in self._store:
            return None
        # Touching an entry counts as "recently used" — move it to the back
        # of the order so it's the last thing considered for eviction.
        self._store.move_to_end(key)
        return self._store[key]

    def set(self, key: str, value: Any) -> None:
        self._store[key] = value
        self._store.move_to_end(key)

        if len(self._store) > self.max_size:
            # last=False pops from the FRONT — the least-recently-used entry,
            # since every get()/set() pushes its key to the back.
            self._store.popitem(last=False)

    def invalidate(self, key: str) -> None:
        self._store.pop(key, None)

# Short answer: because computing a neighbor list is expensive 
# (search candidates, compute distance to each, run heap eviction),
#  but reading an already-computed list should be cheap and fast.

# Without a separate GET, every single person asking "who's similar 
# to X" would trigger that whole expensive computation again, 
# even though the answer hasn't changed since the last time. 
# Indexing (POST) does the expensive work once and saves the result. 
# GET just looks up that saved result — a simple, fast read, nothing recomputed.

# That split — expensive write, cheap read — is also exactly why the cache exists: 
# to make the "cheap read" even cheaper.

# => can i say that this is the replacement for the [ fetch_candidates ] ,
#  to avoid a neighbouring candidates search ??????????????????????????????????
# No — they're not related at all, and it's worth being precise here since 
# this is the third time this mix-up has come up.

# fetch_candidates is only ever called during indexing (writes) — 
# POST /neighbors/{id}/index.
# It's never called during a GET request at all.

# The cache only sits in front of GET requests —
# reading an answer that was already fully computed and saved earlier.

# So they don't even share the same code path — one runs on write, 
# the other runs on read. The cache can't "replace" fetch_candidates '
# 'because fetch_candidates never runs anywhere near where the cache lives. '
# 'It replaces nothing about the search — it only avoids repeating a Postgres '
# 'read of an answer that hasn't changed.