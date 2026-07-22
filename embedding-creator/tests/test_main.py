from collections.abc import Iterator

import numpy as np
import pytest
from fastapi.testclient import TestClient

from app.main import app

# pytest → test framework
# TestClient → sends fake HTTP requests to the FastAPI app (no real server needed)
# numpy → computes cosine similarity
# app → imports the FastAPI application from main.py 


@pytest.fixture(scope="module")
def client() -> Iterator[TestClient]:
    with TestClient(app) as test_client:
        yield test_client


def cosine_similarity(a: list[float], b: list[float]) -> float:
    a_arr, b_arr = np.array(a), np.array(b)
    return float(np.dot(a_arr, b_arr) / (np.linalg.norm(a_arr) * np.linalg.norm(b_arr)))


def test_health_returns_200(client: TestClient) -> None:
    response = client.get("/health")
    assert response.status_code == 200


def test_embed_returns_384_dim_vector_with_matching_entity_id(
    client: TestClient,
) -> None:
    response = client.post(
        "/embed", json={"entityId": "N1", "text": "A basketball game recap."}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["entityId"] == "N1"
    assert len(body["vector"]) == 384


def test_similar_articles_are_more_cosine_similar_than_unrelated(
    client: TestClient,
) -> None:
    sports_1 = client.post(
        "/embed",
        json={
            "entityId": "N1",
            "text": "Lakers win championship game in overtime thriller",
        },
    ).json()
    sports_2 = client.post(
        "/embed",
        json={
            "entityId": "N2",
            "text": "NBA finals game ends in dramatic overtime victory",
        },
    ).json()
    cooking = client.post(
        "/embed",
        json={
            "entityId": "N3",
            "text": "Simple recipe for baking sourdough bread at home",
        },
    ).json()

    similar_score = cosine_similarity(sports_1["vector"], sports_2["vector"])
    unrelated_score = cosine_similarity(sports_1["vector"], cooking["vector"])

    assert similar_score > unrelated_score



# client fixture — uses with TestClient(app) as test_client:, not a bare 
# TestClient(app). This matters: FastAPI/Starlette only actually run the 
# lifespan startup/shutdown code (the part that loads the model into _model) 
# when the client is used as a context manager. Without the with, _model would
# stay None for every test, and /embed would hit the RuntimeError("Model is 
# not loaded") branch we wrote in main.py. scope="module" means the model
#  loads once for the whole file, not once per test — loading all-MiniLM-L6-v2 isn't free, and none of these tests need a fresh model instance.
# cosine_similarity — plain numpy, no library dependency beyond what's already in requirements.txt. Standard formula: dot product over the product of magnitudes.

# Test 1 (/health) — the simplest possible smoke test.
# Test 2 — checks the exact contract shape from Deliverable A: same entityId comes back, vector is exactly 384 long. This is really a contract test — it'd catch someone accidentally changing EMBEDDING_DIM or breaking the alias serialization we discussed.
# Test 3 — the real one. Two sports headlines, one cooking headline, three real /embed calls (this test genuinely runs the model — it's not mocked), then asserts similar_score > unrelated_score. This is your spec's own verification criterion, written as code instead of something you'd have to check by hand every time.
# That's the whole test file — three tests, no mocking of the model itself since the point is to prove the real embeddings behave sensibly. Next candidate would be embedding-creator/Dockerfile, or — since you said "assume main.py runs and is okay" — we could stop assuming and actually run pytest for real now to check that assumption. Which do you want

# Without with:

# client = TestClient(app)

# the lifespan may not run, so:

# _model = None

# and every /embed request would fail with:

# RuntimeError("Model is not loaded")