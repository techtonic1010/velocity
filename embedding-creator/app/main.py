from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from pydantic import BaseModel, ConfigDict, Field
from sentence_transformers import SentenceTransformer

# embed(): _model.encode(text, convert_to_numpy=True) returns a numpy array; 
# the if vector.shape[0] 
# != EMBEDDING_DIM check is a deliberate guardrail — if someone ever swaps 
# MODEL_NAME to a model with a different output dimension, this fails loudly 
# at the one call site instead of silently writing wrong-sized vectors into 
# Postgres later.
# Note there's no try/except around _model.encode(...) — FastAPI already 
# turns any unhandled exception into a 500, and there's nothing meaningful
#  this service could do to recover from an encode failure, so I didn't add
#  handling for a case that can't be usefully handled here.

MODEL_NAME = "all-MiniLM-L6-v2"
EMBEDDING_DIM = 384

_model: SentenceTransformer | None = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    global _model
    _model = SentenceTransformer(MODEL_NAME)
    yield
    _model = None

# 5. Create FastAPI app
app = FastAPI(title="Embedding Creator", lifespan=lifespan)


class EmbedRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    entity_id: str = Field(alias="entityId")
    text: str


class EmbedResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    entity_id: str = Field(alias="entityId")
    vector: list[float]


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/embed", response_model=EmbedResponse)
def embed(request: EmbedRequest) -> EmbedResponse:
    if _model is None:
        raise RuntimeError("Model is not loaded")

    vector = _model.encode(request.text, convert_to_numpy=True)
    if vector.shape[0] != EMBEDDING_DIM:
        raise RuntimeError(
            f"Expected a {EMBEDDING_DIM}-dim embedding, got {vector.shape[0]}"
        )

    return EmbedResponse(entity_id=request.entity_id, vector=vector.tolist())

# pytest, httpx, numpy — these three are only for the tests we're about 
# to write, not for main.py itself: pytest is the runner (matches CLAUDE.md's
#  pytest build command), httpx is what FastAPI's TestClient uses under the 
# hood to fake HTTP calls without a real server, and numpy is what the test 
# file will use directly to compute cosine similarity. I put them in the same 
# requirements.txt rather than a separate requirements-dev.txt because this is
#  one small single-purpose service — a second file would be a distinction 
# without a difference here.
