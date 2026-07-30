# Project Structure

```
├── database
│   └── init
│       ├── 001_schema.sql
│       ├── 002_neighbor_index_schema.sql
│       └── 003_entity_history_schema.sql
├── embedding-creator
│   ├── app
│   │   ├── __init__.py
│   │   └── main.py
│   ├── tests
│   │   └── test_main.py
│   ├── Dockerfile
│   └── requirements.txt
├── entity-interaction-service
│   ├── bin
│   │   ├── src
│   │   │   └── main
│   │   │       ├── java
│   │   │       │   └── com
│   │   │       └── resources
│   │   └── target
│   │       ├── classes
│   │       │   └── com
│   │       │       └── velocity
│   │       ├── generated-sources
│   │       │   └── annotations
│   │       └── maven-status
│   │           └── maven-compiler-plugin
│   │               └── compile
│   ├── src
│   │   ├── main
│   │   │   ├── java
│   │   │   │   └── com
│   │   │   │       └── velocity
│   │   │   └── resources
│   │   │       └── application.yml
│   │   └── test
│   │       └── java
│   │           └── com
│   │               └── velocity
│   ├── Dockerfile
│   ├── pom.xml
│   └── REQUEST_FLOW.md
├── entity-upload-service
│   ├── src
│   │   └── main
│   │       ├── java
│   │       │   └── com
│   │       │       └── velocity
│   │       └── resources
│   │           └── application.yml
│   ├── Dockerfile
│   └── pom.xml
├── recommendation-service
│   ├── src
│   │   ├── main
│   │   │   ├── java
│   │   │   │   └── com
│   │   │   │       └── velocity
│   │   │   └── resources
│   │   │       └── application.yml
│   │   └── test
│   │       └── java
│   │           └── com
│   │               └── velocity
│   ├── Dockerfile
│   └── pom.xml
├── vector-hasher
│   ├── app
│   │   ├── __init__.py
│   │   ├── cache.py
│   │   ├── db.py
│   │   ├── heap.py
│   │   ├── kafka_client.py
│   │   ├── lsh.py
│   │   ├── main.py
│   │   └── neighbor_index.py
│   ├── tests
│   │   ├── __init__.py
│   │   ├── README.md
│   │   ├── test_heap.py
│   │   └── test_lsh.py
│   ├── Dockerfile
│   └── requirements.txt
├── CLAUDE.md
├── docker-compose.yml
├── MILESTONE_3_PLAN.md
├── MILESTONE_4_PLAN.md
├── MILESTONE_5_PLAN.md
├── PROGRESS.md
└── PROJECT_SPEC.md
```

# File Contents

## database/init/001_schema.sql

```sql
-- Milestone 1: Entity DB schema.
-- Auto-executed by Postgres on first boot (docker-entrypoint-initdb.d), see docker-compose.yml.
-- NOTE: only runs against a fresh postgres-data volume. If this table is missing after a
-- previous `docker-compose up`, the volume already existed — run `docker-compose down -v`
-- and re-up, or apply this file manually.

CREATE TABLE IF NOT EXISTS entities (
    entity_id   VARCHAR(32)  PRIMARY KEY,
    title       TEXT         NOT NULL,
    abstract    TEXT,
    category    VARCHAR(100),
    subcategory VARCHAR(100),
    -- 384 x 4-byte little-endian float32, packed, no header (numpy/ByteBuffer contract).
    vector      BYTEA        NOT NULL,
    -- Populated by Milestone 2's LSH hasher; unused (always 0) until then.
    shard_id    INTEGER      NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- entity_id	N12345
-- title	   49ers trade for Deshaun Watson in stunning offseason move
-- abstract	   San Francisco sends two first-round picks as part of a blockbuster deal that reshapes the NFC playoff picture.
-- category	   sports
-- subcategory	football_nfl
-- vector	   \x8a3f4c3e... (1,536 raw bytes — see below)
-- shard_id	    7
-- created_at	2026-07-20 14:32:11.402+00
```

## database/init/002_neighbor_index_schema.sql

```sql
-- Milestone 3: Neighbor Index write-side + read-side schema.
-- Auto-executed by Postgres on first boot (docker-entrypoint-initdb.d), see docker-compose.yml.
-- NOTE: only runs against a fresh postgres-data volume, same caveat as 001_schema.sql.

-- Write side: updated directly by vector-hasher whenever it indexes an entity
-- (builds/evicts its max-heap). Never read by the serving path directly.
CREATE TABLE IF NOT EXISTS neighbor_index_write (
    entity_id   VARCHAR(32)  PRIMARY KEY,
    shard_id    INTEGER      NOT NULL,
    -- Flattened, closest-first: [{"entityId": "...", "distance": 0.42}, ...]
    neighbors   JSONB        NOT NULL,
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- compute (search candidates → cosine distance → heap eviction) 
-- → save to neighbor_index_write (what you just described) → 
-- produce to Kafka → consumer picks it up → 
-- saves to neighbor_index_read (the separate table GET /neighbors and the cache actually read from) 
-- → cache invalidated for that entity.

-- So neighbor_index_write is the landing spot right after computation, 
-- and the only way data ever reaches neighbor_index_read is through Kafka 
-- — never a direct write. That separation is deliberate, 
-- matching the "write-side heap maintenance is never read directly by serving" 
-- idea from earlier.

-- Read side: only ever written by the Kafka consumer, overwriting the row
-- wholesale on every message (full list, not a diff — idempotent, safe to replay).
CREATE TABLE IF NOT EXISTS neighbor_index_read (
    entity_id   VARCHAR(32)  PRIMARY KEY,
    neighbors   JSONB        NOT NULL,
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

```

## database/init/003_entity_history_schema.sql

```sql
-- Milestone 4: Entity History schema (durable record of user clicks/likes/dislikes).
-- Auto-executed by Postgres on first boot (docker-entrypoint-initdb.d), see docker-compose.yml.
-- NOTE: only runs against a fresh postgres-data volume, same caveat as 001_schema.sql /
-- 002_neighbor_index_schema.sql — this project's Postgres has already run once, so this
-- file also needs to be applied by hand (psql) against the live database.

CREATE TABLE IF NOT EXISTS entity_history (
    source_id         VARCHAR(64)  NOT NULL,   -- real MIND impression ID (sitting clicks) or a
                                                 -- made-up marker (background-history clicks) —
                                                 -- identifies exactly which real record this came from
    entity_id         VARCHAR(32)  NOT NULL,
    user_id           VARCHAR(32)  NOT NULL,
    interaction_type  VARCHAR(10)  NOT NULL,   -- CLICK | LIKE | DISLIKE
    event_timestamp   TIMESTAMPTZ  NOT NULL,
    shard_id          INTEGER      NOT NULL,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    -- (source_id, entity_id) together identify one real event — replaying the same
    -- event twice (redelivery, re-run) upserts the existing row instead of duplicating it.
    PRIMARY KEY (source_id, entity_id)
);

-- PK (source_id, entity_id) only gives a fast path when source_id is known.
-- These cover the two other lookup patterns Milestone 5 needs: by article, and by time.
CREATE INDEX IF NOT EXISTS idx_entity_history_entity_id ON entity_history (entity_id);
CREATE INDEX IF NOT EXISTS idx_entity_history_event_timestamp ON entity_history (event_timestamp);

```

## embedding-creator/Dockerfile

```
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2')"

# Normally, the first time your code runs:

# SentenceTransformer("all-MiniLM-L6-v2")

# it downloads the model from Hugging Face.

# That can take several seconds and requires internet access.

# Instead, this Dockerfile downloads it during the image build.

COPY app/ app/

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]


# This is a Dockerfile. It tells Docker how to build a Docker image for your FastAPI embedding service.

# Think of it as a recipe:

# "Start with this OS + Python, install these dependencies, copy my code, and when the container starts, run my application."

# Let's go line by line.
```

## embedding-creator/app/__init__.py

```python

```

## embedding-creator/app/main.py

```python
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

```

## embedding-creator/requirements.txt

```txt
fastapi==0.115.0

# uvicorn[standard]

# because it installs optional performance libraries like

# uvloop
# httptools
# websockets
# watchfiles

uvicorn[standard]==0.30.6
sentence-transformers==3.0.1
pydantic==2.9.2
numpy==1.26.4
pytest==8.3.3
httpx==0.27.2

# What this file is and why it's needed now: embedding-creator/requirements.txt. Nothing in this folder can execute yet — main.py imports fastapi, pydantic, sentence_transformers, none of which are installed anywhere. This file is what pip install -r requirements.txt reads locally, and it's also what the (not-yet-written) Dockerfile's dependency-layer will install.
# Python does not automatically install libraries that your code imports.

# For example, if main.py contains

# from fastapi import FastAPI
# from pydantic import BaseModel
# from sentence_transformers import SentenceTransformer
# python main.py
# Python immediately throws

# ModuleNotFoundError: No module named 'fastapi'

# Anyone can now install everything with one command:

# pip install -r requirements.txt

```

## embedding-creator/tests/test_main.py

```python
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
```

## entity-interaction-service/Dockerfile

```
# --- Build stage ---
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace

COPY pom.xml .
RUN mvn -B dependency:go-offline

COPY src/ src/
RUN mvn -B clean package -DskipTests

# --- Runtime stage ---
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /workspace/target/entity-interaction-service-*.jar app.jar

EXPOSE 8080
CMD ["java", "-jar", "app.jar"]

```

## entity-interaction-service/REQUEST_FLOW.md

````markdown
# Entity Interaction Service — Request Flow

One HTTP endpoint (`POST /replay`) that ends up writing to two independent stores (Postgres, Redis), with Kafka as the hand-off point between the "produce" half and the "consume" half of the system. Those two halves know nothing about each other directly — only Kafka connects them.

## End-to-end flow

```
                              HTTP
                               │
                               ▼
                 ┌──────────────────────────┐
                 │     ReplayController        │   POST /replay?userId=... | ?limit=...
                 └──────────────┬─────────────┘
                                 │ userId / limit
                                 ▼
                 ┌──────────────────────────┐
                 │       ReplayService          │   reads both TSV files, groups rows by user
                 └──────────────┬─────────────┘
                                 │ List<BehaviorRow>   (all of one user's rows)
                                 ▼
                 ┌──────────────────────────┐
                 │    UserTimelineBuilder       │   sort → History events → real-click events
                 └──────────────┬─────────────┘
                                │ List<InteractionEvent>   (chronologically ordered)
                                ▼
                 ┌──────────────────────────┐
                 │  InteractionEventProducer    │   kafkaTemplate.send(topic, key=userId, event)
                 └──────────────┬─────────────┘
                                 │
                  ══════════════╪══════════════   Kafka topic "interaction-events"  (key = userId)
                                 │
                                 ▼
                 ┌──────────────────────────     ┐
                 │  InteractionEventConsumer     │   @KafkaListener, batch mode
                 └───┬───────┬───────┬────────┬─ ┘
       (per event) ┌─┘       │       │        └─┐ (once, after the loop over the whole batch)
                    ▼         ▼       ▼          ▼
           LastNCacheService  BloomFilterService  SignalsService   EntityHistoryRepository
                    │         │       │                            │
                    ▼         ▼       ▼                            ▼
                 Redis      Redis   Redis                       Postgres
             (lastEntities)(bloomfilter)(signals)             (entity_history)
```

## Classes: inputs, outputs, who calls whom

| # | Class | Trigger / input | Calls | Output |
|---|---|---|---|---|
| 1 | `ReplayController` | HTTP `POST /replay?userId=` or `?limit=` | `ReplayService.replayUser(userId)` or `.replayAll(limit)` | `ReplayResult` → JSON response body |
| 2 | `ReplayService` | `userId: String` or `limit: Integer` | `BehaviorsTsvParser.parse(...)`, `UserTimelineBuilder.build(...)`, `InteractionEventProducer.publish(...)` per event | `ReplayResult(usersReplayed, eventsPublished)` |
| 3 | `BehaviorsTsvParser` | raw TSV `Reader` + split label | — (pure parsing) | `Stream<BehaviorRow>` |
| 4 | `UserTimelineBuilder` | `userId`, `List<BehaviorRow>` | `MurmurHash3.hash32(...)` (LIKE/DISLIKE classification) | `List<InteractionEvent>`, ordered |
| 5 | `InteractionEventProducer` | one `InteractionEvent` | `KafkaTemplate.send(topic, userId, event)` | none — fire-and-forget |
| 6 | `InteractionEventConsumer` | Kafka delivers a batch on `interaction-events` | `LastNCacheService`, `BloomFilterService`, `SignalsService` per event; `EntityHistoryRepository.upsertBatch(...)` once per batch | none — side effects only |
| 7 | `LastNCacheService` | `userId, entityId` | one atomic Lua script (`LREM`→`LPUSH`→`LTRIM`) | new list length (unused) |
| 8 | `BloomFilterService` | `userId, entityId` | `MurmurHash3.hash32(...)` ×2, then 7× `SETBIT` | none |
| 9 | `SignalsService` | `userId, entityId, interactionType` | one `HSET` | none |
| 10 | `EntityHistoryRepository` | `List<InteractionEvent>`, `redisShardCount` | `ShardUtil.shardFor(...)` per row, one JDBC batch `INSERT ... ON CONFLICT DO NOTHING` | none |

## Data shapes that actually cross a boundary

**`InteractionEvent`** — the one shape that crosses every boundary in the system: it's the Kafka message value, what `UserTimelineBuilder` produces, and the row shape written to Postgres.
```
{ userId, entityId, interactionType: CLICK|LIKE|DISLIKE, timestamp, sourceId }
```

**`BehaviorRow`** — internal only, never crosses Kafka, never leaves the producer side.
```
{ impressionId, split: TRAIN|DEV, userId, time, historyEntityIds: [...], impressions: [{entityId, clicked}, ...] }
```

**`ReplayResult`** — the only shape that ever reaches the HTTP caller.
```
{ usersReplayed: int, eventsPublished: int }
```

## The one thing worth drawing explicitly

Everything left of Kafka (`ReplayController` → `InteractionEventProducer`) has zero knowledge of Postgres or Redis. Everything right of Kafka (`InteractionEventConsumer` → the four downstream services) has zero knowledge of TSV files or replay logic. Kafka is the only link — which is what makes the write side (Redis + Postgres) able to fall behind or restart without losing anything: every published event sits on the topic until a consumer is there to read it.

````

## entity-interaction-service/pom.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>3.3.4</version>
        <relativePath/>
    </parent>

    <groupId>com.velocity</groupId>
    <artifactId>entity-interaction-service</artifactId>
    <version>0.1.0</version>
    <name>entity-interaction-service</name>
    <description>Milestone 4 : entity-interaction-service which , takes the user events and pushes into the db and redis both </description>

    <properties>
        <java.version>21</java.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-jdbc</artifactId>
        </dependency>
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
            <scope>runtime</scope>
        </dependency>
        
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-data-redis</artifactId>
        </dependency>

        <!-- Actuator -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-actuator</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>

```

## entity-interaction-service/src/main/java/com/velocity/entityinteraction/EntityInteractionServiceApplication.java

```java
package com.velocity.entityinteraction;



import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class EntityInteractionServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(EntityInteractionServiceApplication.class, args);
    }
}
```

## entity-interaction-service/src/main/java/com/velocity/entityinteraction/config/KafkaConsumerConfig.java

```java
package com.velocity.entityinteraction.config;

import com.velocity.entityinteraction.dto.InteractionEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.Map;

import javax.swing.Spring;

@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    // Every operation InteractionEventConsumer performs per batch (Redis ring-buffer/Bloom/signals
    // updates, the Postgres ON CONFLICT DO NOTHING upsert) is idempotent — see that class's own
    // comment — so there's no "poison message" risk here, only transient dependency outages
    // (Redis/Postgres briefly down). Retrying forever with a capped backoff is therefore safe and
    // correct; giving up early (Spring Kafka's out-of-the-box default) silently drops real data.
    private static final long RETRY_INITIAL_INTERVAL_MS = 1_000L;
    private static final double RETRY_MULTIPLIER = 2.0;
    private static final long RETRY_MAX_INTERVAL_MS = 30_000L;
    // Creates a ConsumerFactory, which acts as a blueprint for creating Kafka consumers.
    @Bean
    public ConsumerFactory<String, InteractionEvent> interactionEventConsumerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
                // only from the specified package for security.
        JsonDeserializer<InteractionEvent> valueDeserializer = new JsonDeserializer<>(InteractionEvent.class);
        valueDeserializer.addTrustedPackages("com.velocity.entityinteraction.dto");

        Map<String, Object> configProps = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "entity-interaction-service",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new DefaultKafkaConsumerFactory<>(configProps, new StringDeserializer(), valueDeserializer);
    }

    // No maxElapsedTime is set, so this backs off exponentially up to RETRY_MAX_INTERVAL_MS and
    // then keeps retrying forever at that interval — it never gives up and never needs a
    // recoverer, since a recoverer only runs once retries are exhausted.
    static ExponentialBackOff interactionEventBackOff() {
        ExponentialBackOff backOff = new ExponentialBackOff(RETRY_INITIAL_INTERVAL_MS, RETRY_MULTIPLIER);
        backOff.setMaxInterval(RETRY_MAX_INTERVAL_MS);
        return backOff;
    }

    @Bean
    public DefaultErrorHandler interactionEventErrorHandler() {
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(interactionEventBackOff());
        // RetryListener has two failedDelivery overloads: a single-ConsumerRecord one (the only
        // abstract method, so the only one a lambda can implement) and a default no-op
        // ConsumerRecords (plural) one. batchFactory is a *batch* listener, so
        // ErrorHandlingUtils.retryBatch always calls the plural overload — a lambda here would
        // silently never log anything. Must override it explicitly.
        errorHandler.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
                // Never called for a batch listener; present only to satisfy the interface.
            }

            @Override
            public void failedDelivery(ConsumerRecords<?, ?> records, Exception ex, int deliveryAttempt) {
                log.warn("Retrying interaction-events batch of {} (attempt {}) after: {}",
                        records.count(), deliveryAttempt, ex.getMessage());
            }
        });
        return errorHandler;
    }

    // Batch listener: InteractionEventConsumer processes a whole poll's worth of records at once —
    // Redis updates happen per-event, the Postgres write happens once per batch.

    // ConcurrentKafkaListenerContainerFactory → A factory that creates listener containers.
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, InteractionEvent> batchFactory(
            ConsumerFactory<String, InteractionEvent> interactionEventConsumerFactory,
            DefaultErrorHandler interactionEventErrorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, InteractionEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
         // Whenever you need a KafkaConsumer, create it using this ConsumerFactory
        factory.setConsumerFactory(interactionEventConsumerFactory);
        factory.setBatchListener(true);
        factory.setCommonErrorHandler(interactionEventErrorHandler);
        // Spring calls it only once:

            // consume(
            //     [event1,
            //      event2,
            //      ...
            //      event100]
            // )

            // Much faster for bulk processing.
            // Instead of:

// 100 INSERT statements

// you can do:

// 1 bulk INSERT
        return factory;
    }
}

```

## entity-interaction-service/src/main/java/com/velocity/entityinteraction/config/KafkaHealthIndicator.java

```java
package com.velocity.entityinteraction.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class KafkaHealthIndicator implements HealthIndicator {

    private final KafkaAdmin kafkaAdmin;

    public KafkaHealthIndicator(KafkaAdmin kafkaAdmin) {
        this.kafkaAdmin = kafkaAdmin;
    }

    @Override
    public Health health() {
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            int nodeCount = adminClient.describeCluster().nodes().get(2, TimeUnit.SECONDS).size();
            return Health.up().withDetail("nodes", nodeCount).build();
        } catch (Exception e) {
            return Health.down(e).build();
        }
    }
}
// /home/parth-ratnaparkhi/Desktop/Velocity/entity-interaction-service/src/main/java/com/velocity/entityinteraction/config/KafkaHealthIndicator.java
```

## entity-interaction-service/src/main/java/com/velocity/entityinteraction/config/KafkaProducerConfig.java

```java
package com.velocity.entityinteraction.config;

import com.velocity.entityinteraction.dto.InteractionEvent;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    // Spring Boot's default value serializer is StringSerializer, which can't handle a record —
    // JsonSerializer is what lets KafkaTemplate<String, InteractionEvent> actually serialize the payload.

//     String: The data type of the message key (usually an ID or partition key).
// InteractionEvent: The data type of the message value (your Java object/record containing the payload).
    @Bean
    public ProducerFactory<String, InteractionEvent> interactionEventProducerFactory(
        // / 1. Spring creates the Factory Bean
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> configProps = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                // Uses StringSerializer. In Kafka, message keys are used for partitioning and are usually simple strings (like a user ID or event ID).
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                // Uses Spring's JsonSerializer (from org.springframework.kafka.support.serializer). This is the key piece—it takes your InteractionEvent Java object/record and serializes it into a JSON byte array before sending it over the wire.
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, InteractionEvent> interactionEventKafkaTemplate(
            ProducerFactory<String, InteractionEvent> interactionEventProducerFactory) {
        return new KafkaTemplate<>(interactionEventProducerFactory);
    }
}

```

## entity-interaction-service/src/main/java/com/velocity/entityinteraction/config/RedisConfig.java

```java
package com.velocity.entityinteraction.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

@Configuration
public class RedisConfig {

    // LREM (remove any existing copy) + LPUSH (push to front) + LTRIM (trim to size) as ONE Lua
    // script, so Redis runs all three atomically — no other update to the same key can interleave
    // between them. Returns the list's final length (LLEN) so the caller gets a real Long back,
    // since LTRIM's own reply ("OK") isn't a usable return type here.

    // So LLEN is mainly there to provide a numeric return value,
    //  not because the ring buffer logic depends on it.
    @Bean
    public RedisScript<Long> lastNRingBufferScript() {
        String script = """
                redis.call('LREM', KEYS[1], 0, ARGV[1])
                redis.call('LPUSH', KEYS[1], ARGV[1])
                redis.call('LTRIM', KEYS[1], 0, ARGV[2])
                return redis.call('LLEN', KEYS[1])
                """;
        return new DefaultRedisScript<>(script, Long.class);
    }
}

```

## entity-interaction-service/src/main/java/com/velocity/entityinteraction/controller/HealthController.java

```java
package com.velocity.entityinteraction.controller;


import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HealthController{
    @GetMapping("/health")
    public String healthCheck() {
        return "Entity Interaction Service is healthy!";
    }
}

```

## entity-interaction-service/src/main/java/com/velocity/entityinteraction/controller/InteractionController.java

```java
package com.velocity.entityinteraction.controller;

import com.velocity.entityinteraction.dto.InteractionEvent;
import com.velocity.entityinteraction.service.InteractionEventProducer;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class InteractionController {

    private final InteractionEventProducer producer;

    public InteractionController(InteractionEventProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/interactions")
    public ResponseEntity<Void> submit(@RequestBody InteractionEvent event) {
        producer.publish(event);
        return ResponseEntity.status(HttpStatus.ACCEPTED).build();
    }
}

```

## entity-interaction-service/src/main/java/com/velocity/entityinteraction/controller/ReplayController.java

```java
package com.velocity.entityinteraction.controller;

import com.velocity.entityinteraction.service.ReplayService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ReplayController {

    private final ReplayService replayService;

    public ReplayController(ReplayService replayService) {
        this.replayService = replayService;
    }

    @PostMapping("/replay")
    public ReplayService.ReplayResult replay(
            @RequestParam(required = false) String userId,
            @RequestParam(required = false) Integer limit) {
        if (userId != null) {
            return replayService.replayUser(userId);
        }
        return replayService.replayAll(limit);
    }
}

```

## entity-interaction-service/src/main/java/com/velocity/entityinteraction/dto/InteractionEvent.java

```java
package com.velocity.entityinteraction.dto;

import java.time.Instant;

// This is the message shape that goes onto the Kafka interaction-events topic, keyed by userId.
// It's also the exact JSON body POST /interactions expects — same record, no separate
// request DTO, since the wire shape and the Kafka payload are identical.
//
// sourceId is what Postgres's entity_history PRIMARY KEY (source_id, entity_id) relies on for
// idempotency, so it must be assigned per the replay's rules, not left to the caller's discretion:
//   - real click (from an impression row)  -> "{SPLIT}-{impressionId}"  e.g. "TRAIN-8821", "DEV-341"
//   - synthetic click (from History replay) -> "HIST-{userId}-{index}"  e.g. "HIST-U13740-0"
// Split-prefixing avoids train/dev impressionId collisions; per-item History indices keep genuine
// repeated History entries as distinct permanent rows instead of upserting over each other.
//
// Example:
// {
//   "userId": "U131",
//   "entityId": "N12345",
//   "interactionType": "CLICK",
//   "timestamp": "2019-11-13T08:36:57Z",
//   "sourceId": "TRAIN-8821"
// }
public record InteractionEvent(
        String userId,
        String entityId,
        InteractionType interactionType,
        Instant timestamp,
        String sourceId) {
}
// add the sample DTO class for the interaction event, 
// which will be used to send the event to Kafka.



```

## entity-interaction-service/src/main/java/com/velocity/entityinteraction/dto/InteractionType.java

```java
package com.velocity.entityinteraction.dto;

// Matches entity_history.interaction_type (VARCHAR(10)) in 003_entity_history_schema.sql.
// Jackson serializes/deserializes this as the plain enum name — e.g. "interactionType": "CLICK"
// in JSON — with zero extra annotations, and that same string is what lands in the DB column.
public enum InteractionType {
    CLICK,
    LIKE,
    DISLIKE
}

```

## entity-interaction-service/src/main/java/com/velocity/entityinteraction/model/BehaviorRow.java

```java
package com.velocity.entityinteraction.model;

import java.time.LocalDateTime;
import java.util.List;

// One raw row of MIND's behaviors.tsv: ImpressionID, UserID, Time, History, Impressions.
// `split` ("TRAIN" or "DEV") is set by the parser, not read from the file — it's what lets sourceId
// stay unique across the two files, since ImpressionID alone collides between them.
public record BehaviorRow(
        String impressionId,
        String split,
        String userId,
        LocalDateTime time,
        List<String> historyEntityIds,
        List<ImpressionEntry> impressions) {

    public record ImpressionEntry(String entityId, boolean clicked) {
    }
}

```

## entity-interaction-service/src/main/java/com/velocity/entityinteraction/parser/BehaviorsTsvParser.java

```java
package com.velocity.entityinteraction.parser;

import com.velocity.entityinteraction.model.BehaviorRow;
import com.velocity.entityinteraction.model.BehaviorRow.ImpressionEntry;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

/**
 * Parses MIND's behaviors.tsv: tab-separated, no header row.
 * Columns (verified against the real downloaded file): ImpressionID, UserID, Time, History,
 * Impressions. Time is e.g. "11/11/2019 9:05:58 AM" — not always zero-padded.
 */
public final class BehaviorsTsvParser {

    private static final int EXPECTED_COLUMNS = 5;
    private static final DateTimeFormatter TIME_FORMAT =
            DateTimeFormatter.ofPattern("M/d/yyyy h:mm:ss a", Locale.ENGLISH);

    private BehaviorsTsvParser() {
    }

    public static Stream<BehaviorRow> parse(Reader reader, String split) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(reader);
        return bufferedReader.lines()
                .filter(line -> !line.isBlank())
                .map(line -> parseLine(line, split));
    }

    private static BehaviorRow parseLine(String line, String split) {
        String[] columns = line.split("\t", -1);
        if (columns.length < EXPECTED_COLUMNS) {
            throw new IllegalArgumentException(
                    "behaviors.tsv row has fewer than " + EXPECTED_COLUMNS + " columns: " + line);
        }

        String impressionId = columns[0];
        String userId = columns[1];
        LocalDateTime time = LocalDateTime.parse(columns[2], TIME_FORMAT);
        List<String> historyEntityIds = parseHistory(columns[3]);
        List<ImpressionEntry> impressions = parseImpressions(columns[4]);

        return new BehaviorRow(impressionId, split, userId, time, historyEntityIds, impressions);
    }

    private static List<String> parseHistory(String rawHistory) {
        if (rawHistory == null || rawHistory.isBlank()) {
            return List.of();
        }
        return List.of(rawHistory.split(" "));
    }

    private static List<ImpressionEntry> parseImpressions(String rawImpressions) {
        List<ImpressionEntry> impressions = new ArrayList<>();
        for (String token : rawImpressions.split(" ")) {
            int dashIndex = token.lastIndexOf('-');
            String entityId = token.substring(0, dashIndex);
            boolean clicked = "1".equals(token.substring(dashIndex + 1));
            impressions.add(new ImpressionEntry(entityId, clicked));
        }
        return impressions;
    }
}

```

## entity-interaction-service/src/main/java/com/velocity/entityinteraction/repository/EntityHistoryRepository.java

```java
package com.velocity.entityinteraction.repository;

import com.velocity.entityinteraction.dto.InteractionEvent;
import com.velocity.entityinteraction.util.ShardUtil;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.util.List;

@Repository
public class EntityHistoryRepository {

    private static final String UPSERT_SQL = """
            INSERT INTO entity_history (source_id, entity_id, user_id, interaction_type, event_timestamp, shard_id)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (source_id, entity_id) DO NOTHING
            """;

    private final JdbcTemplate jdbcTemplate;

    public EntityHistoryRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    // entity_history is an PAST historical-fact table — a redelivered/ replayed identical event
    // has identical field values, so DO NOTHING (first write wins) is correct, not DO UPDATE.
    public void upsertBatch(List<InteractionEvent> events, int redisShardCount) {
        jdbcTemplate.batchUpdate(UPSERT_SQL, events, events.size(), (ps, event) -> {
            ps.setString(1, event.sourceId());
            ps.setString(2, event.entityId());
            ps.setString(3, event.userId());
            ps.setString(4, event.interactionType().name());
            ps.setTimestamp(5, Timestamp.from(event.timestamp()));
            ps.setInt(6, ShardUtil.shardFor(event.userId(), redisShardCount));
        });
    }
}

```

## entity-interaction-service/src/main/java/com/velocity/entityinteraction/service/BloomFilterService.java

```java
package com.velocity.entityinteraction.service;

import com.velocity.entityinteraction.util.MurmurHash3;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

@Service
public class BloomFilterService {

    // Sized for n=100 expected entities/user, p=0.01 false-positive rate:
    // m = -(n * ln p) / (ln 2)^2 ~= 960 bits, k = (m/n) * ln 2 ~= 7 hash functions.
    // Real data shows p99=137 entities/user, which exceeds this design capacity for the heaviest
    // users — false-positive rate creeps up for them, which is safe (fallback to DB) but worth
    // tracking; not fixed here (see PROJECT_SPEC.md #10).
    private static final int BIT_SIZE = 960;
    private static final int HASH_COUNT = 7;

    private final StringRedisTemplate redisTemplate;

    public BloomFilterService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void add(String userId, String entityId) {
        String key = "user:" + userId + ":bloomfilter";
        for (int bitPosition : bitPositions(userId, entityId)) {
            redisTemplate.opsForValue().setBit(key, bitPosition, true);
        }
    }

    public boolean mightContain(String userId, String entityId) {
        String key = "user:" + userId + ":bloomfilter";
        for (int bitPosition : bitPositions(userId, entityId)) {
            if (!Boolean.TRUE.equals(redisTemplate.opsForValue().getBit(key, bitPosition))) {
                return false;
            }
        }
        return true;
    }

    // Kirsch-Mitzenmacher double hashing: derive k bit positions from just 2 real hash calls
    // instead of k separate ones — h_i(x) = h1(x) + i*h2(x) mod m, statistically as good as k
    // independent hashes for Bloom filter purposes.
    private int[] bitPositions(String userId, String entityId) {
        byte[] data = (userId + "|" + entityId).getBytes(StandardCharsets.UTF_8);
        int h1 = MurmurHash3.hash32(data, 0);
        int h2 = MurmurHash3.hash32(data, 1);

        int[] positions = new int[HASH_COUNT];
        for (int i = 0; i < HASH_COUNT; i++) {
            positions[i] = Math.floorMod(h1 + i * h2, BIT_SIZE);
        }
        return positions;
    }
}

```

## entity-interaction-service/src/main/java/com/velocity/entityinteraction/service/InteractionEventConsumer.java

```java
package com.velocity.entityinteraction.service;

import com.velocity.entityinteraction.dto.InteractionEvent;
import com.velocity.entityinteraction.repository.EntityHistoryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;

// Single consumer doing both jobs: per-event Redis updates (idempotent, safe to repeat), then one
// batched Postgres write. If the batch write throws, Spring Kafka redelivers the whole batch —
// redoing the already-idempotent Redis updates on redelivery is harmless.
@Service
public class InteractionEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(InteractionEventConsumer.class);

    private final LastNCacheService lastNCacheService;
    private final BloomFilterService bloomFilterService;
    private final SignalsService signalsService;
    private final EntityHistoryRepository entityHistoryRepository;
    private final int redisShardCount;

    public InteractionEventConsumer(
            LastNCacheService lastNCacheService,
            BloomFilterService bloomFilterService,
            SignalsService signalsService,
            EntityHistoryRepository entityHistoryRepository,
            @Value("${redis.shard-count}") int redisShardCount) {
        this.lastNCacheService = lastNCacheService;
        this.bloomFilterService = bloomFilterService;
        this.signalsService = signalsService;
        this.entityHistoryRepository = entityHistoryRepository;
        this.redisShardCount = redisShardCount;
    }

    @KafkaListener(topics = "${interaction.kafka-topic}", containerFactory = "batchFactory")
    public void consume(List<InteractionEvent> batch) {
        // Iterate in received order — a Kafka partition preserves per-key (per-user) order, so
        // processing the batch in order keeps one user's events chronologically correct.
        for (InteractionEvent event : batch) {
            lastNCacheService.recordClick(event.userId(), event.entityId());
            bloomFilterService.add(event.userId(), event.entityId());
            signalsService.record(event.userId(), event.entityId(), event.interactionType());
        }
        entityHistoryRepository.upsertBatch(batch, redisShardCount);
        log.info("Consumed batch of {} interaction events", batch.size());
    }
}

// InteractionEventConsumer.consume(batch)
//         │
//         ├─ for each event in the batch, in order:
//         │       lastNCacheService.recordClick(userId, entityId)   → runs the Lua script
//         │       bloomFilterService.add(userId, entityId)          → 7x SETBIT
//         │       signalsService.record(userId, entityId, type)     → 1x HSET
//         │
//         └─ once, after the loop:
//                 entityHistoryRepository.upsertBatch(batch, redisShardCount)  → 1 JDBC batch INSERT

// Nothing calls InteractionEventConsumer directly in your code — 
// Spring Kafka's container (built from batchFactory) invokes .consume(...) 
// automatically whenever new messages show up on the topic, because of the
//  @KafkaListener annotation on that method.


```

## entity-interaction-service/src/main/java/com/velocity/entityinteraction/service/InteractionEventProducer.java

```java
package com.velocity.entityinteraction.service;

import com.velocity.entityinteraction.dto.InteractionEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class InteractionEventProducer {
// class fields (variables) are private to encapsulate the state of the class 
// and prevent external modification. This is a fundamental principle of
//  object-oriented programming that promotes data integrity and security.
    private final KafkaTemplate<String, InteractionEvent> kafkaTemplate;
    private final String topic;

//     KafkaTemplate is reusable across different topics

// Imagine your app later needs to publish UserRegistrationEvent or AuditLogEvent.
   
public InteractionEventProducer(
    // 1. Spring checks its memory: "Do I have a bean matching KafkaTemplate<String, InteractionEvent>?"
            KafkaTemplate<String, InteractionEvent> interactionEventKafkaTemplate,
            @Value("${interaction.kafka-topic}") String topic) {
        this.kafkaTemplate = interactionEventKafkaTemplate;
        this.topic = topic;
    }
// This automatic passing of dependencies from the configuration class into your service class is called Dependency Injection

    // Keyed by userId so Kafka preserves per-user ordering across the topic's partitions.
    public void publish(InteractionEvent event) {
        kafkaTemplate.send(topic, event.userId(), event);
    }
}

```

## entity-interaction-service/src/main/java/com/velocity/entityinteraction/service/LastNCacheService.java

```java
package com.velocity.entityinteraction.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class LastNCacheService {

    private final StringRedisTemplate redisTemplate;
    private final RedisScript<Long> lastNRingBufferScript;
    private final int lastNSize;

    public LastNCacheService(
            StringRedisTemplate redisTemplate,
            RedisScript<Long> lastNRingBufferScript,
            @Value("${redis.last-n-size}") int lastNSize) {
        this.redisTemplate = redisTemplate;
        this.lastNRingBufferScript = lastNRingBufferScript;
        this.lastNSize = lastNSize;
    }

    public void recordClick(String userId, String entityId) {
        String key = "user:" + userId + ":lastEntities";
        redisTemplate.execute(lastNRingBufferScript, List.of(key), entityId, String.valueOf(lastNSize - 1));
    }
}

// You need LastNCacheService because it encapsulates all Redis "last N entities" logic in one place.

// Instead of every class doing this:

// String key = "user:" + userId + ":lastEntities";
// redisTemplate.execute(script, ...);

// they simply call:

// lastNCacheService.recordClick(userId, entityId);

```

## entity-interaction-service/src/main/java/com/velocity/entityinteraction/service/ReplayService.java

```java
package com.velocity.entityinteraction.service;

import com.velocity.entityinteraction.dto.InteractionEvent;
import com.velocity.entityinteraction.model.BehaviorRow;
import com.velocity.entityinteraction.parser.BehaviorsTsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

// Reads MIND's real behaviors.tsv (both splits) and replays it through Kafka as if arriving live —
// the "interaction replay" scope decision from PROJECT_SPEC.md §2. Only talks to
// InteractionEventProducer directly (in-process), never loops back over HTTP to POST /interactions.
@Service
public class ReplayService {

    private static final Logger log = LoggerFactory.getLogger(ReplayService.class);

    private final InteractionEventProducer producer;
    private final Path trainTsvPath;
    private final Path devTsvPath;

    public ReplayService(
            InteractionEventProducer producer,
            @Value("${mind.behaviors-train-tsv-path}") String trainTsvPath,
            @Value("${mind.behaviors-dev-tsv-path}") String devTsvPath) {
        this.producer = producer;
        this.trainTsvPath = Path.of(trainTsvPath);
        this.devTsvPath = Path.of(devTsvPath);
    }

    /**
     * Replays exactly one real user's click history — Milestone 4's literal verify criterion.
     * Scans both splits for rows belonging to userId, builds the merged timeline, publishes
     * every event in order.
     */
    public ReplayResult replayUser(String userId) {
        List<BehaviorRow> rows = new ArrayList<>();
        rows.addAll(readRowsForUser(trainTsvPath, "TRAIN", userId));
        rows.addAll(readRowsForUser(devTsvPath, "DEV", userId));

        if (rows.isEmpty()) {
            return new ReplayResult(0, 0);
        }

        List<InteractionEvent> events = UserTimelineBuilder.build(userId, rows);
        events.forEach(producer::publish);
        log.info("Replayed userId={}: {} events published", userId, events.size());
        return new ReplayResult(1, events.size());
    }

    /**
     * Bulk replay, bounded by limit (mirrors entity-upload-service's ?limit= convention) — reads
     * up to limit rows from each split, groups by user, replays every user found.
     */
    public ReplayResult replayAll(Integer limit) {
        Map<String, List<BehaviorRow>> byUser = new LinkedHashMap<>();
        readRows(trainTsvPath, "TRAIN", limit, byUser);
        readRows(devTsvPath, "DEV", limit, byUser);

        int eventsPublished = 0;
        for (Map.Entry<String, List<BehaviorRow>> entry : byUser.entrySet()) {
            List<InteractionEvent> events = UserTimelineBuilder.build(entry.getKey(), entry.getValue());
            events.forEach(producer::publish);
            eventsPublished += events.size();
        }
        log.info("Replayed {} users, {} events published", byUser.size(), eventsPublished);
        return new ReplayResult(byUser.size(), eventsPublished);
    }

    private List<BehaviorRow> readRowsForUser(Path path, String split, String userId) {
        try (FileReader reader = new FileReader(path.toFile());
             Stream<BehaviorRow> rows = BehaviorsTsvParser.parse(reader, split)) {
            return rows.filter(row -> row.userId().equals(userId)).toList();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read behaviors.tsv at " + path, e);
        }
    }

    private void readRows(Path path, String split, Integer limit, Map<String, List<BehaviorRow>> byUser) {
        try (FileReader reader = new FileReader(path.toFile());
             Stream<BehaviorRow> stream = BehaviorsTsvParser.parse(reader, split)) {
            Stream<BehaviorRow> bounded = limit != null ? stream.limit(limit) : stream;
            bounded.forEach(row -> byUser.computeIfAbsent(row.userId(), id -> new ArrayList<>()).add(row));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read behaviors.tsv at " + path, e);
        }
    }

    public record ReplayResult(int usersReplayed, int eventsPublished) {
    }
}

```

## entity-interaction-service/src/main/java/com/velocity/entityinteraction/service/SignalsService.java

```java
package com.velocity.entityinteraction.service;

import com.velocity.entityinteraction.dto.InteractionType;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

@Service
public class SignalsService {

    private final StringRedisTemplate redisTemplate;

    public SignalsService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    // One entry per (user, entity) pair. Includes CLICK, not just LIKE/DISLIKE — deviates from
    // PROJECT_SPEC.md #5.4's literal shape, per this session's explicit decision.
    public void record(String userId, String entityId, InteractionType type) {
        String key = "user:" + userId + ":signals";
        redisTemplate.opsForHash().put(key, entityId, type.name());
    }
}

```

## entity-interaction-service/src/main/java/com/velocity/entityinteraction/service/UserTimelineBuilder.java

```java
package com.velocity.entityinteraction.service;

import com.velocity.entityinteraction.dto.InteractionEvent;
import com.velocity.entityinteraction.dto.InteractionType;
import com.velocity.entityinteraction.model.BehaviorRow;
import com.velocity.entityinteraction.model.BehaviorRow.ImpressionEntry;
import com.velocity.entityinteraction.util.MurmurHash3;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Pure logic, no I/O: turns one user's raw behaviors.tsv rows (both splits combined) into the
 * merged, chronological list of InteractionEvents that gets published to Kafka.
 */
public final class UserTimelineBuilder {

    private static final double DISLIKE_THRESHOLD = 0.05;
    private static final double LIKE_THRESHOLD = 0.15;

    private UserTimelineBuilder() {
    }

    public static List<InteractionEvent> build(String userId, List<BehaviorRow> rows) {
        List<BehaviorRow> sorted = rows.stream()
                .sorted(Comparator.comparing(BehaviorRow::time))
                .toList();

        List<InteractionEvent> events = new ArrayList<>();
        events.addAll(buildHistoryEvents(userId, sorted));
        events.addAll(buildRealClickEvents(userId, sorted));
        return events;
    }

    private static List<InteractionEvent> buildHistoryEvents(String userId, List<BehaviorRow> sortedRows) {
        // History is confirmed frozen across all of a user's rows (spot-checked against real data) —
        // the earliest row after sorting is as good a source as any, they're all identical.
        List<String> historyEntityIds = sortedRows.get(0).historyEntityIds();
        // Anchors to the earliest impression row's Time (not earliest click) — always defined, even
        // for a user whose impressions never got a real click.
        Instant anchor = sortedRows.get(0).time().toInstant(ZoneOffset.UTC);

        int size = historyEntityIds.size();
        List<InteractionEvent> events = new ArrayList<>(size);
        for (int index = 0; index < size; index++) {
            String entityId = historyEntityIds.get(index);
            Instant timestamp = anchor.minusSeconds(size - index);
            String sourceId = "HIST-" + userId + "-" + index;
            events.add(new InteractionEvent(userId, entityId, InteractionType.CLICK, timestamp, sourceId));
        }
        return events;
    }

    private static List<InteractionEvent> buildRealClickEvents(String userId, List<BehaviorRow> sortedRows) {
        List<InteractionEvent> events = new ArrayList<>();
        for (BehaviorRow row : sortedRows) {
            Instant timestamp = row.time().toInstant(ZoneOffset.UTC);
            for (ImpressionEntry impression : row.impressions()) {
                if (!impression.clicked()) {
                    continue;
                }
                String sourceId = row.split() + "-" + row.impressionId();
                InteractionType type = classify(userId, impression.entityId());
                events.add(new InteractionEvent(userId, impression.entityId(), type, timestamp, sourceId));
            }
        }
        return events;
    }

    /**
     * Deterministic LIKE/DISLIKE simulation, real clicks only — History-derived events are always
     * plain CLICK (handled above). Reuses the same MurmurHash3 primitive the Bloom filter uses.
     * Same (userId, entityId) pair always yields the same label, since a real pair can be clicked in
     * two separate real sessions and both must agree.
     */
    private static InteractionType classify(String userId, String entityId) {
        String key = userId + "|" + entityId;
        int hash = MurmurHash3.hash32(key.getBytes(StandardCharsets.UTF_8), 0);
        double bucket = (hash & 0xFFFFFFFFL) / 4294967296.0;
        if (bucket < DISLIKE_THRESHOLD) {
            return InteractionType.DISLIKE;
        }
        if (bucket < LIKE_THRESHOLD) {
            return InteractionType.LIKE;
        }
        return InteractionType.CLICK;
    }
}

```

## entity-interaction-service/src/main/java/com/velocity/entityinteraction/util/MurmurHash3.java

```java
package com.velocity.entityinteraction.util;

// Hand-rolled MurmurHash3 x86-32 (Austin Appleby's public-domain algorithm) — deliberately not an
// external library, since this project has no hashing dependency and both call sites (the
// LIKE/DISLIKE simulation and the Bloom filter) only need a fast, deterministic, well-distributed hash.
public final class MurmurHash3 {

    private static final int C1 = 0xcc9e2d51;
    private static final int C2 = 0x1b873593;

    private MurmurHash3() {
    }

    public static int hash32(byte[] data, int seed) {
        int hash = seed;
        int length = data.length;
        int blockCount = length / 4;

        for (int i = 0; i < blockCount; i++) {
            int k = (data[i * 4] & 0xff)
                    | ((data[i * 4 + 1] & 0xff) << 8)
                    | ((data[i * 4 + 2] & 0xff) << 16)
                    | ((data[i * 4 + 3] & 0xff) << 24);
            hash = mixBlock(hash, k);
        }

        int tailStart = blockCount * 4;
        int tail = 0;
        switch (length - tailStart) {
            case 3:
                tail ^= (data[tailStart + 2] & 0xff) << 16;
            case 2:
                tail ^= (data[tailStart + 1] & 0xff) << 8;
            case 1:
                tail ^= (data[tailStart] & 0xff);
                tail *= C1;
                tail = Integer.rotateLeft(tail, 15);
                tail *= C2;
                hash ^= tail;
        }

        hash ^= length;
        return finalize(hash);
    }

    private static int mixBlock(int hash, int k) {
        k *= C1;
        k = Integer.rotateLeft(k, 15);
        k *= C2;
        hash ^= k;
        hash = Integer.rotateLeft(hash, 13);
        return hash * 5 + 0xe6546b64;
    }

    private static int finalize(int h) {
        h ^= h >>> 16;
        h *= 0x85ebca6b;
        h ^= h >>> 13;
        h *= 0xc2b2ae35;
        h ^= h >>> 16;
        return h;
    }
}

```

## entity-interaction-service/src/main/java/com/velocity/entityinteraction/util/REPLAY_PIPELINE.md

````markdown
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

````

## entity-interaction-service/src/main/java/com/velocity/entityinteraction/util/ShardUtil.java

```java
package com.velocity.entityinteraction.util;

// Logical sharding only (per PROJECT_SPEC.md §2's scope decision) — there's one Redis container in
// docker-compose.yml, this just computes/records which shard would own a user in a real deployment.
// A distinct concept from vector-hasher's NUM_SHARDS (LSH bucket count) — don't conflate the two.
public final class ShardUtil {

    private ShardUtil() {
    }

    public static int shardFor(String userId, int numShards) {
        return Math.floorMod(userId.hashCode(), numShards);
    }
}

```

## entity-interaction-service/src/main/resources/application.yml

```yaml
server:
  port: 8080

spring:
  application:
    name: entity-interaction-service
  datasource:
    url: ${SPRING_DATASOURCE_URL}
    username: ${SPRING_DATASOURCE_USERNAME}
    password: ${SPRING_DATASOURCE_PASSWORD}
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS}
  data:
    redis:
      host: ${REDIS_HOST}
      port: ${REDIS_PORT}

interaction:
  kafka-topic: ${KAFKA_INTERACTION_TOPIC}

mind:
  behaviors-train-tsv-path: ${MIND_BEHAVIORS_TRAIN_TSV_PATH}
  behaviors-dev-tsv-path: ${MIND_BEHAVIORS_DEV_TSV_PATH}

redis:
  last-n-size: ${LAST_N_SIZE}
  shard-count: ${REDIS_SHARD_COUNT}

management:
  endpoints:
    web:
      exposure:
        include: health
  endpoint:
    health:
      show-details: always

```

## entity-interaction-service/src/test/java/com/velocity/entityinteraction/config/KafkaConsumerConfigTest.java

```java
package com.velocity.entityinteraction.config;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaConsumerConfigTest {

    @Test
    void backOffRetriesForeverWithExponentialDelayCappedAtThirtySeconds() {
        ExponentialBackOff backOff = KafkaConsumerConfig.interactionEventBackOff();

        assertThat(backOff.getInitialInterval()).isEqualTo(1_000L);
        assertThat(backOff.getMultiplier()).isEqualTo(2.0);
        assertThat(backOff.getMaxInterval()).isEqualTo(30_000L);
        // Neither maxElapsedTime nor maxAttempts is overridden, so both stay at
        // ExponentialBackOff's own unlimited defaults -> retries never exhaust.
        assertThat(backOff.getMaxElapsedTime()).isEqualTo(Long.MAX_VALUE);
        assertThat(backOff.getMaxAttempts()).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void errorHandlerBeanIsBuiltFromThatBackOff() {
        DefaultErrorHandler errorHandler = new KafkaConsumerConfig().interactionEventErrorHandler();

        assertThat(errorHandler).isNotNull();
    }
}

```

## entity-interaction-service/src/test/java/com/velocity/entityinteraction/controller/HealthControllerTest.java

```java
package com.velocity.entityinteraction.controller;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class HealthControllerTest {

    @Test
    void healthCheckReturnsAHealthyMessage() {
        assertThat(new HealthController().healthCheck()).isEqualTo("Entity Interaction Service is healthy!");
    }
}

```

## entity-interaction-service/src/test/java/com/velocity/entityinteraction/controller/InteractionControllerTest.java

```java
package com.velocity.entityinteraction.controller;

import com.velocity.entityinteraction.dto.InteractionEvent;
import com.velocity.entityinteraction.dto.InteractionType;
import com.velocity.entityinteraction.service.InteractionEventProducer;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class InteractionControllerTest {

    @Test
    void submitPublishesTheEventAndReturns202Accepted() {
        InteractionEventProducer producer = mock(InteractionEventProducer.class);
        InteractionController controller = new InteractionController(producer);
        InteractionEvent event = new InteractionEvent(
                "U131", "N45", InteractionType.CLICK, Instant.parse("2019-11-13T08:36:57Z"), "TRAIN-1");

        ResponseEntity<Void> response = controller.submit(event);

        verify(producer).publish(event);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response.getBody()).isNull();
    }
}

```

## entity-interaction-service/src/test/java/com/velocity/entityinteraction/controller/ReplayControllerTest.java

```java
package com.velocity.entityinteraction.controller;

import com.velocity.entityinteraction.service.ReplayService;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class ReplayControllerTest {

    @Test
    void withUserIdPresentDelegatesToReplayUserAndIgnoresLimit() {
        ReplayService replayService = mock(ReplayService.class);
        ReplayService.ReplayResult expected = new ReplayService.ReplayResult(1, 4);
        when(replayService.replayUser("U131")).thenReturn(expected);
        ReplayController controller = new ReplayController(replayService);

        ReplayService.ReplayResult result = controller.replay("U131", 100);

        assertThat(result).isEqualTo(expected);
        verify(replayService).replayUser("U131");
        verify(replayService, never()).replayAll(anyInt());
    }

    @Test
    void withoutUserIdDelegatesToReplayAllWithTheGivenLimit() {
        ReplayService replayService = mock(ReplayService.class);
        ReplayService.ReplayResult expected = new ReplayService.ReplayResult(5, 20);
        when(replayService.replayAll(50)).thenReturn(expected);
        ReplayController controller = new ReplayController(replayService);

        ReplayService.ReplayResult result = controller.replay(null, 50);

        assertThat(result).isEqualTo(expected);
        verify(replayService).replayAll(50);
        verify(replayService, never()).replayUser(anyString());
    }

    @Test
    void withNeitherUserIdNorLimitDelegatesToReplayAllWithNullLimit() {
        ReplayService replayService = mock(ReplayService.class);
        ReplayService.ReplayResult expected = new ReplayService.ReplayResult(10, 40);
        when(replayService.replayAll(null)).thenReturn(expected);
        ReplayController controller = new ReplayController(replayService);

        ReplayService.ReplayResult result = controller.replay(null, null);

        assertThat(result).isEqualTo(expected);
        verify(replayService).replayAll(null);
    }
}

```

## entity-interaction-service/src/test/java/com/velocity/entityinteraction/parser/BehaviorsTsvParserTest.java

```java
package com.velocity.entityinteraction.parser;

import com.velocity.entityinteraction.model.BehaviorRow;
import com.velocity.entityinteraction.model.BehaviorRow.ImpressionEntry;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringReader;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BehaviorsTsvParserTest {

    @Test
    void parsesAllFiveColumnsOfARealRow() throws IOException {
        String line = "1\tU131\t11/13/2019 8:36:57 AM\tN11 N22 N33\tN45-1 N46-0";

        try (Stream<BehaviorRow> rows = BehaviorsTsvParser.parse(new StringReader(line), "TRAIN")) {
            BehaviorRow row = rows.toList().get(0);

            assertThat(row.impressionId()).isEqualTo("1");
            assertThat(row.userId()).isEqualTo("U131");
            assertThat(row.split()).isEqualTo("TRAIN");
            assertThat(row.time()).isEqualTo(LocalDateTime.of(2019, 11, 13, 8, 36, 57));
            assertThat(row.historyEntityIds()).containsExactly("N11", "N22", "N33");
            assertThat(row.impressions()).containsExactly(
                    new ImpressionEntry("N45", true),
                    new ImpressionEntry("N46", false));
        }
    }

    @Test
    void blankHistoryColumnParsesToEmptyList() throws IOException {
        String line = "2\tU200\t11/13/2019 9:05:58 AM\t\tN45-0";

        try (Stream<BehaviorRow> rows = BehaviorsTsvParser.parse(new StringReader(line), "DEV")) {
            BehaviorRow row = rows.toList().get(0);
            assertThat(row.historyEntityIds()).isEmpty();
        }
    }

    @Test
    void blankLinesAreSkipped() throws IOException {
        String tsv = String.join("\n",
                "1\tU131\t11/13/2019 8:36:57 AM\tN11\tN45-1",
                "",
                "2\tU132\t11/13/2019 9:05:58 AM\tN12\tN46-0",
                "   ");

        try (Stream<BehaviorRow> rows = BehaviorsTsvParser.parse(new StringReader(tsv), "TRAIN")) {
            assertThat(rows.toList()).hasSize(2);
        }
    }

    @Test
    void rowWithFewerThanFiveColumnsThrows() {
        String line = "1\tU131\t11/13/2019 8:36:57 AM\tN11";

        assertThatThrownBy(() -> {
            try (Stream<BehaviorRow> rows = BehaviorsTsvParser.parse(new StringReader(line), "TRAIN")) {
                rows.toList();
            }
        }).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void notZeroPaddedTimeIsParsedCorrectly() throws IOException {
        String line = "3\tU9\t1/5/2019 1:02:03 AM\tN1\tN2-1";

        try (Stream<BehaviorRow> rows = BehaviorsTsvParser.parse(new StringReader(line), "TRAIN")) {
            BehaviorRow row = rows.toList().get(0);
            assertThat(row.time()).isEqualTo(LocalDateTime.of(2019, 1, 5, 1, 2, 3));
        }
    }

    @Test
    void multipleImpressionsPreserveOrderAndClickLabels() throws IOException {
        String line = "1\tU131\t11/13/2019 8:36:57 AM\tN11\tN1-0 N2-1 N3-0 N4-1";

        try (Stream<BehaviorRow> rows = BehaviorsTsvParser.parse(new StringReader(line), "TRAIN")) {
            List<ImpressionEntry> impressions = rows.toList().get(0).impressions();
            assertThat(impressions).containsExactly(
                    new ImpressionEntry("N1", false),
                    new ImpressionEntry("N2", true),
                    new ImpressionEntry("N3", false),
                    new ImpressionEntry("N4", true));
        }
    }
}

```

## entity-interaction-service/src/test/java/com/velocity/entityinteraction/repository/EntityHistoryRepositoryTest.java

```java
package com.velocity.entityinteraction.repository;

import com.velocity.entityinteraction.dto.InteractionEvent;
import com.velocity.entityinteraction.dto.InteractionType;
import com.velocity.entityinteraction.util.ShardUtil;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class EntityHistoryRepositoryTest {

    @Test
    void upsertBatchSendsOneBatchUpdateWithCorrectSqlAndBatchSize() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        EntityHistoryRepository repository = new EntityHistoryRepository(jdbcTemplate);
        InteractionEvent event = new InteractionEvent(
                "U131", "N45", InteractionType.CLICK, Instant.parse("2019-11-13T08:36:57Z"), "TRAIN-1");
        List<InteractionEvent> events = List.of(event);

        repository.upsertBatch(events, 8);

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).batchUpdate(sqlCaptor.capture(), eq(events), eq(1), any());
        String sql = sqlCaptor.getValue();
        assertThat(sql).contains("INSERT INTO entity_history");
        assertThat(sql).contains("ON CONFLICT (source_id, entity_id) DO NOTHING");
    }

    @SuppressWarnings("unchecked")
    @Test
    void perRowSetterBindsAllSixColumnsInOrder() throws SQLException {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        EntityHistoryRepository repository = new EntityHistoryRepository(jdbcTemplate);
        InteractionEvent event = new InteractionEvent(
                "U131", "N45", InteractionType.LIKE, Instant.parse("2019-11-13T08:36:57Z"), "TRAIN-1");
        List<InteractionEvent> events = List.of(event);

        repository.upsertBatch(events, 8);

        ArgumentCaptor<ParameterizedPreparedStatementSetter<InteractionEvent>> pssCaptor =
                ArgumentCaptor.forClass(ParameterizedPreparedStatementSetter.class);
        verify(jdbcTemplate).batchUpdate(anyString(), eq(events), eq(1), pssCaptor.capture());

        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        pssCaptor.getValue().setValues(preparedStatement, event);

        int expectedShard = ShardUtil.shardFor("U131", 8);
        verify(preparedStatement).setString(1, "TRAIN-1");
        verify(preparedStatement).setString(2, "N45");
        verify(preparedStatement).setString(3, "U131");
        verify(preparedStatement).setString(4, "LIKE");
        verify(preparedStatement).setTimestamp(5, Timestamp.from(Instant.parse("2019-11-13T08:36:57Z")));
        verify(preparedStatement).setInt(6, expectedShard);
    }
}

```

## entity-interaction-service/src/test/java/com/velocity/entityinteraction/service/BloomFilterServiceTest.java

```java
package com.velocity.entityinteraction.service;

import com.velocity.entityinteraction.util.MurmurHash3;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class BloomFilterServiceTest {

    private static final int BIT_SIZE = 960;
    private static final int HASH_COUNT = 7;

    // Mirrors BloomFilterService's private bitPositions() formula (Kirsch-Mitzenmacher double
    // hashing) using the same public MurmurHash3 utility, so the test can assert exact bit
    // positions instead of just "some bits got set".
    private static int[] computeBitPositions(String userId, String entityId) {
        byte[] data = (userId + "|" + entityId).getBytes(StandardCharsets.UTF_8);
        int h1 = MurmurHash3.hash32(data, 0);
        int h2 = MurmurHash3.hash32(data, 1);
        int[] positions = new int[HASH_COUNT];
        for (int i = 0; i < HASH_COUNT; i++) {
            positions[i] = Math.floorMod(h1 + i * h2, BIT_SIZE);
        }
        return positions;
    }

    @Test
    void addSetsExactlySevenBitsAtTheKirschMitzenmacherPositions() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        ValueOperations<String, String> valueOps = mock(ValueOperations.class);
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        BloomFilterService service = new BloomFilterService(redisTemplate);

        service.add("U131", "N45");

        String expectedKey = "user:U131:bloomfilter";
        int[] expectedPositions = computeBitPositions("U131", "N45");

        ArgumentCaptor<Long> offsetCaptor = ArgumentCaptor.forClass(Long.class);
        verify(valueOps, times(HASH_COUNT)).setBit(eq(expectedKey), offsetCaptor.capture(), eq(true));

        List<Long> capturedOffsets = offsetCaptor.getAllValues();
        assertThat(capturedOffsets).hasSize(HASH_COUNT);
        for (int i = 0; i < HASH_COUNT; i++) {
            assertThat(capturedOffsets.get(i)).isEqualTo((long) expectedPositions[i]);
        }
    }

    @Test
    void mightContainReturnsTrueWhenAllSevenBitsAreSet() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        ValueOperations<String, String> valueOps = mock(ValueOperations.class);
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(valueOps.getBit(anyString(), anyLong())).thenReturn(true);
        BloomFilterService service = new BloomFilterService(redisTemplate);

        assertThat(service.mightContain("U131", "N45")).isTrue();
    }

    @Test
    void mightContainReturnsFalseAndShortCircuitsOnFirstUnsetBit() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        ValueOperations<String, String> valueOps = mock(ValueOperations.class);
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        // First position checked (i=0) comes back unset; every other position would be "set" but
        // must never be reached because the loop returns as soon as it hits a false.
        when(valueOps.getBit(anyString(), anyLong())).thenReturn(false, true, true, true, true, true, true);
        BloomFilterService service = new BloomFilterService(redisTemplate);

        boolean result = service.mightContain("U131", "N45");

        assertThat(result).isFalse();
        verify(valueOps, times(1)).getBit(anyString(), anyLong());
    }

    @Test
    void mightContainTreatsNullBitAsUnset() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        ValueOperations<String, String> valueOps = mock(ValueOperations.class);
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(valueOps.getBit(anyString(), anyLong())).thenReturn(null);
        BloomFilterService service = new BloomFilterService(redisTemplate);

        assertThat(service.mightContain("U131", "N45")).isFalse();
    }
}

```

## entity-interaction-service/src/test/java/com/velocity/entityinteraction/service/InteractionEventConsumerTest.java

```java
package com.velocity.entityinteraction.service;

import com.velocity.entityinteraction.dto.InteractionEvent;
import com.velocity.entityinteraction.dto.InteractionType;
import com.velocity.entityinteraction.repository.EntityHistoryRepository;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.time.Instant;
import java.util.List;

import static org.mockito.Mockito.*;

class InteractionEventConsumerTest {

    private static InteractionEvent event(String userId, String entityId, InteractionType type, String sourceId) {
        return new InteractionEvent(userId, entityId, type, Instant.parse("2019-11-13T08:36:57Z"), sourceId);
    }

    @Test
    void eachEventUpdatesAllThreeRedisServicesThenTheBatchIsWrittenOnceToPostgres() {
        LastNCacheService lastNCacheService = mock(LastNCacheService.class);
        BloomFilterService bloomFilterService = mock(BloomFilterService.class);
        SignalsService signalsService = mock(SignalsService.class);
        EntityHistoryRepository entityHistoryRepository = mock(EntityHistoryRepository.class);
        InteractionEventConsumer consumer = new InteractionEventConsumer(
                lastNCacheService, bloomFilterService, signalsService, entityHistoryRepository, 8);

        InteractionEvent first = event("U1", "N1", InteractionType.CLICK, "TRAIN-1");
        InteractionEvent second = event("U1", "N2", InteractionType.LIKE, "TRAIN-2");
        List<InteractionEvent> batch = List.of(first, second);

        consumer.consume(batch);

        verify(lastNCacheService).recordClick("U1", "N1");
        verify(lastNCacheService).recordClick("U1", "N2");
        verify(bloomFilterService).add("U1", "N1");
        verify(bloomFilterService).add("U1", "N2");
        verify(signalsService).record("U1", "N1", InteractionType.CLICK);
        verify(signalsService).record("U1", "N2", InteractionType.LIKE);
        verify(entityHistoryRepository, times(1)).upsertBatch(batch, 8);
    }

    @Test
    void eventsAreProcessedInBatchOrderAndPostgresWriteHappensAfterAllRedisUpdates() {
        LastNCacheService lastNCacheService = mock(LastNCacheService.class);
        BloomFilterService bloomFilterService = mock(BloomFilterService.class);
        SignalsService signalsService = mock(SignalsService.class);
        EntityHistoryRepository entityHistoryRepository = mock(EntityHistoryRepository.class);
        InteractionEventConsumer consumer = new InteractionEventConsumer(
                lastNCacheService, bloomFilterService, signalsService, entityHistoryRepository, 8);

        InteractionEvent first = event("U1", "N1", InteractionType.CLICK, "TRAIN-1");
        InteractionEvent second = event("U2", "N2", InteractionType.CLICK, "TRAIN-2");
        List<InteractionEvent> batch = List.of(first, second);

        consumer.consume(batch);

        InOrder order = inOrder(lastNCacheService, bloomFilterService, signalsService, entityHistoryRepository);
        order.verify(lastNCacheService).recordClick("U1", "N1");
        order.verify(bloomFilterService).add("U1", "N1");
        order.verify(signalsService).record("U1", "N1", InteractionType.CLICK);
        order.verify(lastNCacheService).recordClick("U2", "N2");
        order.verify(bloomFilterService).add("U2", "N2");
        order.verify(signalsService).record("U2", "N2", InteractionType.CLICK);
        order.verify(entityHistoryRepository).upsertBatch(batch, 8);
    }

    @Test
    void emptyBatchStillCallsUpsertBatchOnceWithEmptyList() {
        LastNCacheService lastNCacheService = mock(LastNCacheService.class);
        BloomFilterService bloomFilterService = mock(BloomFilterService.class);
        SignalsService signalsService = mock(SignalsService.class);
        EntityHistoryRepository entityHistoryRepository = mock(EntityHistoryRepository.class);
        InteractionEventConsumer consumer = new InteractionEventConsumer(
                lastNCacheService, bloomFilterService, signalsService, entityHistoryRepository, 8);

        consumer.consume(List.of());

        verifyNoInteractions(lastNCacheService, bloomFilterService, signalsService);
        verify(entityHistoryRepository).upsertBatch(List.of(), 8);
    }
}

```

## entity-interaction-service/src/test/java/com/velocity/entityinteraction/service/InteractionEventProducerTest.java

```java
package com.velocity.entityinteraction.service;

import com.velocity.entityinteraction.dto.InteractionEvent;
import com.velocity.entityinteraction.dto.InteractionType;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;

import static org.mockito.Mockito.*;

class InteractionEventProducerTest {

    @Test
    void publishSendsToConfiguredTopicKeyedByUserId() {
        KafkaTemplate<String, InteractionEvent> kafkaTemplate = mock(KafkaTemplate.class);
        InteractionEventProducer producer = new InteractionEventProducer(kafkaTemplate, "interaction-events");
        InteractionEvent event = new InteractionEvent(
                "U131", "N45", InteractionType.CLICK, Instant.parse("2019-11-13T08:36:57Z"), "TRAIN-1");

        producer.publish(event);

        verify(kafkaTemplate).send("interaction-events", "U131", event);
    }
}

```

## entity-interaction-service/src/test/java/com/velocity/entityinteraction/service/LastNCacheServiceTest.java

```java
package com.velocity.entityinteraction.service;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.List;

import static org.mockito.Mockito.*;

class LastNCacheServiceTest {

    @Test
    void recordClickRunsTheRingBufferScriptWithKeyEntityIdAndSizeMinusOne() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        RedisScript<Long> script = mock(RedisScript.class);
        LastNCacheService service = new LastNCacheService(redisTemplate, script, 5);

        service.recordClick("U131", "N45");

        // lastNSize=5 -> LTRIM upper bound is size-1=4, since the buffer is a 0-indexed max size.
        verify(redisTemplate).execute(script, List.of("user:U131:lastEntities"), "N45", "4");
    }

    @Test
    void differentUsersGetIndependentKeys() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        RedisScript<Long> script = mock(RedisScript.class);
        LastNCacheService service = new LastNCacheService(redisTemplate, script, 5);

        service.recordClick("U1", "N1");
        service.recordClick("U2", "N1");

        verify(redisTemplate).execute(script, List.of("user:U1:lastEntities"), "N1", "4");
        verify(redisTemplate).execute(script, List.of("user:U2:lastEntities"), "N1", "4");
    }
}

```

## entity-interaction-service/src/test/java/com/velocity/entityinteraction/service/ReplayServiceTest.java

```java
package com.velocity.entityinteraction.service;

import com.velocity.entityinteraction.dto.InteractionEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

// ReplayService takes its TSV paths as plain constructor strings (raw @Value injection), so it can
// be unit-tested directly against real temp files without a Spring context or Kafka/Postgres/Redis.
class ReplayServiceTest {

    @TempDir
    Path tempDir;

    private Path trainTsv;
    private Path devTsv;

    @BeforeEach
    void writeFixtureFiles() throws IOException {
        // U1: History N11,N22 in the earliest (TRAIN) row, real click N45 in TRAIN, real click N50 in DEV.
        // U2: History N30, real click N31 in TRAIN only.
        trainTsv = tempDir.resolve("train-behaviors.tsv");
        Files.writeString(trainTsv, String.join("\n",
                "1\tU1\t11/13/2019 8:36:57 AM\tN11 N22\tN45-1 N46-0",
                "2\tU2\t11/13/2019 9:00:00 AM\tN30\tN31-1"));

        devTsv = tempDir.resolve("dev-behaviors.tsv");
        Files.writeString(devTsv, "3\tU1\t11/14/2019 8:00:00 AM\t\tN50-1");
    }

    @Test
    void replayUserMergesRowsFromBothSplitsInChronologicalOrder() {
        InteractionEventProducer producer = mock(InteractionEventProducer.class);
        ReplayService replayService = new ReplayService(producer, trainTsv.toString(), devTsv.toString());

        ReplayService.ReplayResult result = replayService.replayUser("U1");

        // 2 History events (N11, N22) + 1 TRAIN real click (N45) + 1 DEV real click (N50).
        assertThat(result.usersReplayed()).isEqualTo(1);
        assertThat(result.eventsPublished()).isEqualTo(4);

        ArgumentCaptor<InteractionEvent> captor = ArgumentCaptor.forClass(InteractionEvent.class);
        verify(producer, times(4)).publish(captor.capture());
        List<InteractionEvent> published = captor.getAllValues();

        assertThat(published).extracting(InteractionEvent::sourceId)
                .containsExactly("HIST-U1-0", "HIST-U1-1", "TRAIN-1", "DEV-3");
        assertThat(published).extracting(InteractionEvent::entityId)
                .containsExactly("N11", "N22", "N45", "N50");
        assertThat(published).allMatch(event -> event.userId().equals("U1"));
    }

    @Test
    void replayUserForAnUnknownUserPublishesNothing() {
        InteractionEventProducer producer = mock(InteractionEventProducer.class);
        ReplayService replayService = new ReplayService(producer, trainTsv.toString(), devTsv.toString());

        ReplayService.ReplayResult result = replayService.replayUser("U-does-not-exist");

        assertThat(result.usersReplayed()).isZero();
        assertThat(result.eventsPublished()).isZero();
        verifyNoInteractions(producer);
    }

    @Test
    void replayAllWithoutLimitCoversEveryUserInBothFiles() {
        InteractionEventProducer producer = mock(InteractionEventProducer.class);
        ReplayService replayService = new ReplayService(producer, trainTsv.toString(), devTsv.toString());

        ReplayService.ReplayResult result = replayService.replayAll(null);

        // U1: 4 events (as above). U2: 1 History event (N30) + 1 TRAIN real click (N31) = 2 events.
        assertThat(result.usersReplayed()).isEqualTo(2);
        assertThat(result.eventsPublished()).isEqualTo(6);
        verify(producer, times(6)).publish(any());
    }

    @Test
    void replayAllWithLimitOneOnlyReadsTheFirstRowOfEachFile() {
        InteractionEventProducer producer = mock(InteractionEventProducer.class);
        ReplayService replayService = new ReplayService(producer, trainTsv.toString(), devTsv.toString());

        // limit=1 -> only row 1 (U1) from train, only row 3 (U1) from dev; U2's train row 2 is skipped.
        ReplayService.ReplayResult result = replayService.replayAll(1);

        assertThat(result.usersReplayed()).isEqualTo(1);
        assertThat(result.eventsPublished()).isEqualTo(4);
    }
}

```

## entity-interaction-service/src/test/java/com/velocity/entityinteraction/service/SignalsServiceTest.java

```java
package com.velocity.entityinteraction.service;

import com.velocity.entityinteraction.dto.InteractionType;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import static org.mockito.Mockito.*;

class SignalsServiceTest {

    @Test
    void recordStoresInteractionTypeNameUnderTheUsersSignalsHash() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        HashOperations<String, Object, Object> hashOps = mock(HashOperations.class);
        when(redisTemplate.opsForHash()).thenReturn(hashOps);
        SignalsService service = new SignalsService(redisTemplate);

        service.record("U131", "N45", InteractionType.LIKE);

        verify(hashOps).put("user:U131:signals", "N45", "LIKE");
    }

    @Test
    void clickAndDislikeAreAlsoStoredNotJustLikeDislike() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        HashOperations<String, Object, Object> hashOps = mock(HashOperations.class);
        when(redisTemplate.opsForHash()).thenReturn(hashOps);
        SignalsService service = new SignalsService(redisTemplate);

        service.record("U131", "N45", InteractionType.CLICK);
        service.record("U131", "N46", InteractionType.DISLIKE);

        verify(hashOps).put("user:U131:signals", "N45", "CLICK");
        verify(hashOps).put("user:U131:signals", "N46", "DISLIKE");
    }
}

```

## entity-interaction-service/src/test/java/com/velocity/entityinteraction/service/UserTimelineBuilderTest.java

```java
package com.velocity.entityinteraction.service;

import com.velocity.entityinteraction.dto.InteractionEvent;
import com.velocity.entityinteraction.dto.InteractionType;
import com.velocity.entityinteraction.model.BehaviorRow;
import com.velocity.entityinteraction.model.BehaviorRow.ImpressionEntry;
import com.velocity.entityinteraction.util.MurmurHash3;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class UserTimelineBuilderTest {

    private static final double DISLIKE_THRESHOLD = 0.05;
    private static final double LIKE_THRESHOLD = 0.15;

    // Mirrors UserTimelineBuilder's private classify() formula exactly, using the same public
    // MurmurHash3 utility, so tests can assert real-click labels without hardcoding magic values.
    private static InteractionType expectedType(String userId, String entityId) {
        String key = userId + "|" + entityId;
        int hash = MurmurHash3.hash32(key.getBytes(StandardCharsets.UTF_8), 0);
        double bucket = (hash & 0xFFFFFFFFL) / 4294967296.0;
        if (bucket < DISLIKE_THRESHOLD) {
            return InteractionType.DISLIKE;
        }
        if (bucket < LIKE_THRESHOLD) {
            return InteractionType.LIKE;
        }
        return InteractionType.CLICK;
    }

    @Test
    void historyEntriesProduceClickEventsInOriginalOrderBeforeRealClicks() {
        BehaviorRow row = new BehaviorRow(
                "1", "TRAIN", "U131",
                LocalDateTime.of(2019, 11, 13, 8, 36, 57),
                List.of("N11", "N22"),
                List.of(new ImpressionEntry("N45", true)));

        List<InteractionEvent> events = UserTimelineBuilder.build("U131", List.of(row));

        // 2 history events + 1 real click event.
        assertThat(events).hasSize(3);
        assertThat(events.get(0).entityId()).isEqualTo("N11");
        assertThat(events.get(0).sourceId()).isEqualTo("HIST-U131-0");
        assertThat(events.get(1).entityId()).isEqualTo("N22");
        assertThat(events.get(1).sourceId()).isEqualTo("HIST-U131-1");
        assertThat(events.get(0).interactionType()).isEqualTo(InteractionType.CLICK);
        assertThat(events.get(1).interactionType()).isEqualTo(InteractionType.CLICK);
        // History timestamps strictly precede each other and both precede the real click's row time.
        assertThat(events.get(0).timestamp()).isBefore(events.get(1).timestamp());
        assertThat(events.get(1).timestamp()).isBefore(events.get(2).timestamp());
    }

    @Test
    void onlyClickedImpressionsProduceRealClickEvents() {
        BehaviorRow row = new BehaviorRow(
                "1", "TRAIN", "U131",
                LocalDateTime.of(2019, 11, 13, 8, 36, 57),
                List.of(),
                List.of(new ImpressionEntry("N45", true), new ImpressionEntry("N46", false)));

        List<InteractionEvent> events = UserTimelineBuilder.build("U131", List.of(row));

        assertThat(events).hasSize(1);
        assertThat(events.get(0).entityId()).isEqualTo("N45");
        assertThat(events.get(0).sourceId()).isEqualTo("TRAIN-1");
    }

    @Test
    void realClickTypeMatchesTheDeterministicHashClassification() {
        BehaviorRow row = new BehaviorRow(
                "8821", "TRAIN", "U131",
                LocalDateTime.of(2019, 11, 13, 8, 36, 57),
                List.of(),
                List.of(new ImpressionEntry("N45", true)));

        List<InteractionEvent> events = UserTimelineBuilder.build("U131", List.of(row));

        assertThat(events.get(0).interactionType()).isEqualTo(expectedType("U131", "N45"));
    }

    @Test
    void sameUserEntityPairAlwaysClassifiesTheSameWayAcrossSeparateSessions() {
        BehaviorRow session1 = new BehaviorRow(
                "1", "TRAIN", "U131",
                LocalDateTime.of(2019, 11, 13, 8, 0, 0),
                List.of(),
                List.of(new ImpressionEntry("N45", true)));
        BehaviorRow session2 = new BehaviorRow(
                "2", "DEV", "U131",
                LocalDateTime.of(2019, 11, 14, 8, 0, 0),
                List.of(),
                List.of(new ImpressionEntry("N45", true)));

        List<InteractionEvent> events = UserTimelineBuilder.build("U131", List.of(session1, session2));

        assertThat(events).hasSize(2);
        assertThat(events.get(0).interactionType()).isEqualTo(events.get(1).interactionType());
    }

    @Test
    void historyEventsAreAlwaysClickRegardlessOfHashClassification() {
        // Pick an entityId whose (userId, entityId) pair would NOT classify as CLICK if it went
        // through classify() — History-sourced events must stay CLICK unconditionally anyway.
        String userId = "U999";
        String entityId = "N1";
        InteractionType wouldBeIfRealClick = expectedType(userId, entityId);

        BehaviorRow row = new BehaviorRow(
                "1", "TRAIN", userId,
                LocalDateTime.of(2019, 11, 13, 8, 36, 57),
                List.of(entityId),
                List.of());

        List<InteractionEvent> events = UserTimelineBuilder.build(userId, List.of(row));

        assertThat(events).hasSize(1);
        assertThat(events.get(0).interactionType()).isEqualTo(InteractionType.CLICK);
        // Documents the intentional divergence when the hash-classification would say otherwise.
        if (wouldBeIfRealClick != InteractionType.CLICK) {
            assertThat(events.get(0).interactionType()).isNotEqualTo(wouldBeIfRealClick);
        }
    }

    @Test
    void rowsAreSortedByTimeBeforeHistoryAndRealClickEventsAreBuilt() {
        BehaviorRow later = new BehaviorRow(
                "2", "TRAIN", "U1",
                LocalDateTime.of(2019, 11, 14, 8, 0, 0),
                List.of("N1"),
                List.of(new ImpressionEntry("N9", true)));
        BehaviorRow earlier = new BehaviorRow(
                "1", "TRAIN", "U1",
                LocalDateTime.of(2019, 11, 13, 8, 0, 0),
                List.of("N2"),
                List.of(new ImpressionEntry("N8", true)));

        // Passed in out-of-order deliberately.
        List<InteractionEvent> events = UserTimelineBuilder.build("U1", List.of(later, earlier));

        // History is sourced from the earliest row after sorting, i.e. "earlier" (History="N2").
        InteractionEvent historyEvent = events.stream()
                .filter(e -> e.sourceId().startsWith("HIST-"))
                .findFirst().orElseThrow();
        assertThat(historyEvent.entityId()).isEqualTo("N2");

        // Real click events themselves must appear in ascending row-time order.
        List<InteractionEvent> realClicks = events.stream()
                .filter(e -> !e.sourceId().startsWith("HIST-"))
                .sorted(Comparator.comparing(InteractionEvent::timestamp))
                .toList();
        List<InteractionEvent> realClicksAsBuilt = events.stream()
                .filter(e -> !e.sourceId().startsWith("HIST-"))
                .toList();
        assertThat(realClicksAsBuilt).isEqualTo(realClicks);
        assertThat(realClicksAsBuilt.get(0).sourceId()).isEqualTo("TRAIN-1");
        assertThat(realClicksAsBuilt.get(1).sourceId()).isEqualTo("TRAIN-2");
    }

    @Test
    void emptyHistoryProducesNoHistoryEvents() {
        BehaviorRow row = new BehaviorRow(
                "1", "TRAIN", "U1",
                LocalDateTime.of(2019, 11, 13, 8, 0, 0),
                List.of(),
                List.of(new ImpressionEntry("N1", true)));

        List<InteractionEvent> events = UserTimelineBuilder.build("U1", List.of(row));

        assertThat(events).noneMatch(e -> e.sourceId().startsWith("HIST-"));
    }
}

```

## entity-interaction-service/src/test/java/com/velocity/entityinteraction/util/MurmurHash3Test.java

```java
package com.velocity.entityinteraction.util;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class MurmurHash3Test {

    @Test
    void sameInputAndSeedAlwaysProduceSameHash() {
        byte[] data = "U131|N12345".getBytes(StandardCharsets.UTF_8);

        int first = MurmurHash3.hash32(data, 0);
        int second = MurmurHash3.hash32(data, 0);

        assertThat(first).isEqualTo(second);
    }

    @Test
    void differentSeedsProduceDifferentHashesForSameInput() {
        byte[] data = "U131|N12345".getBytes(StandardCharsets.UTF_8);

        int seed0 = MurmurHash3.hash32(data, 0);
        int seed1 = MurmurHash3.hash32(data, 1);

        assertThat(seed0).isNotEqualTo(seed1);
    }

    @Test
    void differentInputsProduceDifferentHashesForSameSeed() {
        int helloHash = MurmurHash3.hash32("hello".getBytes(StandardCharsets.UTF_8), 0);
        int worldHash = MurmurHash3.hash32("world".getBytes(StandardCharsets.UTF_8), 0);

        assertThat(helloHash).isNotEqualTo(worldHash);
    }

    @Test
    void emptyInputWithZeroSeedHashesToZero() {
        // Derived directly from the algorithm: with no blocks, no tail bytes, and length 0,
        // hash stays 0 through every mixing step, so finalize(0) == 0.
        assertThat(MurmurHash3.hash32(new byte[0], 0)).isZero();
    }

    @Test
    void handlesTailLengthsOfOneTwoAndThreeBytesWithoutError() {
        // Exercises every branch of the fall-through switch in the tail-mixing step (block size is 4).
        for (int length = 1; length <= 3; length++) {
            byte[] data = new byte[length];
            for (int i = 0; i < length; i++) {
                data[i] = (byte) (i + 1);
            }
            int hash = MurmurHash3.hash32(data, 0);
            int hashAgain = MurmurHash3.hash32(data, 0);
            assertThat(hash).isEqualTo(hashAgain);
        }
    }

    @Test
    void changingOneByteChangesTheHash() {
        byte[] data = "N12345".getBytes(StandardCharsets.UTF_8);
        byte[] mutated = "N12346".getBytes(StandardCharsets.UTF_8);

        assertThat(MurmurHash3.hash32(data, 0)).isNotEqualTo(MurmurHash3.hash32(mutated, 0));
    }
}

```

## entity-interaction-service/src/test/java/com/velocity/entityinteraction/util/ShardUtilTest.java

```java
package com.velocity.entityinteraction.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ShardUtilTest {

    @Test
    void sameUserIdAlwaysMapsToSameShard() {
        assertThat(ShardUtil.shardFor("U131", 8)).isEqualTo(ShardUtil.shardFor("U131", 8));
    }

    @Test
    void resultIsAlwaysWithinBounds() {
        String[] userIds = {"U1", "U131", "U9999", "U0", "Uabcxyz", "", "U-negative-hashcode-candidate"};
        int numShards = 8;

        for (String userId : userIds) {
            int shard = ShardUtil.shardFor(userId, numShards);
            assertThat(shard).isGreaterThanOrEqualTo(0).isLessThan(numShards);
        }
    }

    @Test
    void negativeStringHashCodeStillProducesNonNegativeShard() {
        // "polygenelubricants" has a well-known negative String.hashCode(); floorMod (not %) is
        // what guarantees a non-negative shard even when hashCode() is negative.
        int shard = ShardUtil.shardFor("polygenelubricants", 8);
        assertThat(shard).isGreaterThanOrEqualTo(0).isLessThan(8);
    }
}

```

## entity-upload-service/Dockerfile

```
# --- Build stage ---
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace

COPY pom.xml .
RUN mvn -B dependency:go-offline

COPY src/ src/
RUN mvn -B clean package -DskipTests

# --- Runtime stage ---
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /workspace/target/entity-upload-service-*.jar app.jar

EXPOSE 8080
CMD ["java", "-jar", "app.jar"]

```

## entity-upload-service/pom.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>3.3.4</version>
        <relativePath/>
    </parent>

    <groupId>com.velocity</groupId>
    <artifactId>entity-upload-service</artifactId>
    <version>0.1.0</version>
    <name>entity-upload-service</name>
    <description>Milestone 1: MIND article ingestion + embedding orchestration</description>

    <properties>
        <java.version>21</java.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-jdbc</artifactId>
        </dependency>
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
            <scope>runtime</scope>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>

```

## entity-upload-service/src/main/java/com/velocity/entityupload/EntityUploadServiceApplication.java

```java
package com.velocity.entityupload;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class EntityUploadServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(EntityUploadServiceApplication.class, args);
    }
}

// Good, hold off on the large one then — I'll wait for MIND-small. Let's dig into the concurrency design properly.

// What "6 concurrent calls" physically means: Executors.newFixedThreadPool(embedConcurrency) in IngestionService.ingest() creates a pool of exactly 6 real OS threads that live for the duration of one /ingest call. Each of those threads is capable of doing one blocking thing at a time — in our case, one blocking HTTP call to embedding-creator via EmbeddingCreatorClient.embed(). So "6 concurrent" literally means: at most 6 HTTP requests to embedding-creator are ever in flight from this app at the same instant.

// How the other 94 articles in a 100-row batch don't just vanish: in processBatch, the loop executor.submit(() -> embedOne(article)) runs 100 times immediately — submission itself is non-blocking, so all 100 tasks get handed to the pool right away, each returning a Future instantly. But the pool only has 6 threads, so only 6 of those 100 tasks are actually running at once; the other 94 sit in the pool's internal queue. The moment one of the 6 threads finishes its HTTP call, it automatically pulls the next queued task and starts it — this queueing/dispatch is built into ThreadPoolExecutor (what newFixedThreadPool gives you), we didn't write any of that scheduling logic ourselves.

// Why future.get() being called in submission order doesn't waste the concurrency: the second loop in processBatch calls future.get() on futures 1, 2, 3... in the order they were submitted — but that's just the order we check results in, not the order they actually finish in. If article #47's embed call happens to finish before #1's (plausible — network timing varies), it just sits in its already-completed Future waiting for us to get around to asking. All 6 (then rotating through 100) requests are genuinely running concurrently regardless of this checking order.

// Why 6 specifically, and why not more: embedding-creator is one Python process running one loaded model. Model inference (_model.encode(...)) is CPU-bound — it's not like a database that can genuinely serve many parallel queries faster with more connections. If we set embedConcurrency to, say, 50, we wouldn't get 50x throughput — we'd just have 50 requests queued up on FastAPI/Uvicorn's side waiting for the same CPU to free up, with no benefit and more memory/connection overhead on both sides. 6 is a deliberately modest number chosen to keep a few requests overlapping (so one thread isn't sitting completely idle waiting on pure network round-trip time while nothing else happens), without pretending the single-process embedder can actually parallelize real work. Worth being honest: 6 wasn't benchmarked — it's a reasonable starting default for "a handful of overlapping calls to one CPU-bound process," not a number derived from measuring actual throughput.

// One knob you already have for free: embed-concurrency: 6 lives in application.yml, not hardcoded in IngestionService — so if you ever want to experiment (e.g., try 3, or 12) once we're running a real ingest against actual data, it's a one-line config change, no code edit needed.
///////////////////////////////////////////////////////
/// summary : 

/// A thread pool is like a team of 6 workers: you give them all 100 jobs
//  immediately, only 6 work at any moment, the rest wait in a queue, and 
// whenever a worker finishes, it automatically picks up the next waiting
//  job until everything is complete.

///////////////////////////////////////////////////////
```

## entity-upload-service/src/main/java/com/velocity/entityupload/client/EmbeddingCreatorClient.java

```java
package com.velocity.entityupload.client;

import com.velocity.entityupload.dto.EmbedRequest;
import com.velocity.entityupload.dto.EmbedResponse;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

// this is single class responsible for communicating with the embedding-creator service
public class EmbeddingCreatorClient {

    private static final int MAX_ATTEMPTS = 2;
    private static final long RETRY_BACKOFF_MILLIS = 500;

    private final RestClient restClient;

    public EmbeddingCreatorClient(RestClient restClient) {
        this.restClient = restClient;
    }

    public EmbedResponse embed(String entityId, String text) {
        RestClientException lastError = null;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                return restClient.post()
                        .uri("/embed")
                        .body(new EmbedRequest(entityId, text))
                        .retrieve()
                        .body(EmbedResponse.class);
            } catch (RestClientException e) {
                lastError = e;
                if (attempt < MAX_ATTEMPTS) {
                    sleepBeforeRetry();
                }
            }
        }
        throw lastError;
    }

    private void sleepBeforeRetry() {
        try {
            Thread.sleep(RETRY_BACKOFF_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while retrying embed call", e);
        }
    }
}

// Why does EmbeddingCreatorClient exist?

// It is the single class responsible for communicating with the embedding-creator service.

// Instead of IngestionService doing this:

// restClient.post(...)

// it simply does:

// embeddingCreatorClient.embed(...)

// This keeps responsibilities separate.

// EmbeddingCreatorClient → Knows how to call the embedding service (HTTP request, response parsing, retries, error handling).
// IngestionService → Knows what workflow to execute (read 50k articles, batch them, call the client, save results).

// This follows the Single Responsibility Principle (SRP).

// Option 2: Spring creates the RestClient

// Instead:

// public class EmbeddingCreatorClient {

//     private final RestClient restClient;

//     public EmbeddingCreatorClient(RestClient restClient) {
//         this.restClient = restClient;
//     }
// }

// Notice:

// EmbeddingCreatorClient never creates a RestClient.

// It simply says:

// "Someone give me one."

// Who gives it?

// Spring.

// Somewhere else (RestClientConfig)
// @Configuration
// public class RestClientConfig {

//     @Bean
//     RestClient restClient() {
//         return RestClient.builder()
//                 .baseUrl("http://localhost:8000")
//                 .build();
//     }
// }

// Spring executes this once.

// It creates the object.

// Then it injects it into

// EmbeddingCreatorClient

// automatically.
```

## entity-upload-service/src/main/java/com/velocity/entityupload/config/RestClientConfig.java

```java
package com.velocity.entityupload.config;

import com.velocity.entityupload.client.EmbeddingCreatorClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.ClientHttpRequestFactories;
import org.springframework.boot.web.client.ClientHttpRequestFactorySettings;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.time.Duration;

@Configuration
public class RestClientConfig {

    @Bean
    public RestClient embeddingCreatorRestClient(
            @Value("${embedding-creator.base-url}") String baseUrl) {
        ClientHttpRequestFactorySettings settings = ClientHttpRequestFactorySettings.DEFAULTS
                .withConnectTimeout(Duration.ofSeconds(5))
                .withReadTimeout(Duration.ofSeconds(10));
        ClientHttpRequestFactory requestFactory = ClientHttpRequestFactories.get(settings);

        return RestClient.builder()
                .baseUrl(baseUrl)
                .requestFactory(requestFactory)
                .build();
    }

    @Bean
    public EmbeddingCreatorClient embeddingCreatorClient(RestClient embeddingCreatorRestClient) {
        return new EmbeddingCreatorClient(embeddingCreatorRestClient);
    }
}

// First, look at the constructor of EmbeddingCreatorClient

// It probably looks like this:

// public class EmbeddingCreatorClient {

//     private final RestClient restClient;

//     public EmbeddingCreatorClient(RestClient restClient) {
//         this.restClient = restClient;
//     }
// }

// Notice that EmbeddingCreatorClient needs a RestClient to work.

// Why?

// Because EmbeddingCreatorClient itself does not know how to send HTTP requests.

// It uses RestClient whenever you call:

// embeddingCreatorClient.embed(id, text);

// Internally, it does something like:

// restClient.post()
//           .uri("/embed")
//           .body(...)
//           .retrieve()
//           .body(...);

// So,

// EmbeddingCreatorClient
//         │
//         ▼
//    RestClient
//         │
//         ▼
// Python Embedding Service

// Without a RestClient, EmbeddingCreatorClient cannot make HTTP calls.
//////////////////////////////////////////////////////////////////////////////
// Bean 1: RestClient
// @Bean
// public RestClient embeddingCreatorRestClient(...) {
//     ...
// }

// This creates a configured HTTP client.

// Think of it as:

// RestClient
// --------------
// Base URL = http://embedding-creator:8000

// Connect Timeout = 5 sec

// Read Timeout = 10 sec

// This is the configuration.

// Why not inject RestClient directly into IngestionService???

// You could, but then IngestionService would have to know how to make HTTP calls.

// Instead, you keep responsibilities separate:

// IngestionService
//         │
//         ▼
// EmbeddingCreatorClient
//         │
//         ▼
// RestClient
//         │
//         ▼
// Python Service

// Now:

// IngestionService knows "I need an embedding."
// EmbeddingCreatorClient knows "How do I call the Python API?"
// RestClient knows "How do I send HTTP requests?"

// Each class has a single responsibility.
```

## entity-upload-service/src/main/java/com/velocity/entityupload/controller/HealthController.java

```java
package com.velocity.entityupload.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class HealthController {

    @GetMapping("/health")
    public Map<String, String> health() {
        return Map.of("status", "ok");
    }
}

```

## entity-upload-service/src/main/java/com/velocity/entityupload/controller/IngestController.java

```java
package com.velocity.entityupload.controller;

import com.velocity.entityupload.service.IngestionService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class IngestController {

    private final IngestionService ingestionService;

    public IngestController(IngestionService ingestionService) {
        this.ingestionService = ingestionService;
    }

    @PostMapping("/ingest")
    public IngestionService.IngestionResult ingest(
            @RequestParam(required = false) Integer limit) {
        return ingestionService.ingest(limit);
    }
}

// What is IngestionResult?

// Inside IngestionService

// public record IngestionResult(
//     int total,
//     int succeeded,
//     int failed,
//     List<String> failedIds
// ) {}

// This is just a normal Java object.

// Their job is not to do business logic. They simply:

// Accept the HTTP request.
// Extract input (query params, path params, request body).
// Call a service.
// Return whatever the service gives back.
```

## entity-upload-service/src/main/java/com/velocity/entityupload/dto/EmbedRequest.java

```java
package com.velocity.entityupload.dto;
// this is what gets sent as the JSON body of the POST /embed call
public record EmbedRequest(String entityId, String text) {
}

// Records auto-generate equals()/hashCode()/toString() based on their fields — 
// and arrays break that. double[] doesn't override equals() at all 
// (it's reference equality, inherited from Object), so two EmbedResponse records 
// holding the same numbers in a double[] would report as unequal, and toString() would
//  print something useless like [D@1a2b3c4 instead of the actual values.
//      List<Double> behaves the way you'd actually want in tests, logs, and debugging — 
//      this bit us nowhere yet, but it's exactly the kind of thing that's invisible until 
//      the one time you diff two responses in a test and can't figure out why they don't 
//      match.
```

## entity-upload-service/src/main/java/com/velocity/entityupload/dto/EmbedResponse.java

```java
package com.velocity.entityupload.dto;

import java.util.List;

// EmbedResponse(String entityId, List<Double> vector) 
// — the one real decision here is List<Double> rather than double[]. Two reasons:


// Why List<Double>?

// Lists compare contents, not references.
// Why not double[]?

// Records automatically generate:

// equals()
// hashCode()
// toString()

// But arrays don't behave well with these methods.

public record EmbedResponse(String entityId, List<Double> vector) {
}

// What they are and why: these mirror the exact JSON contract main.
// py's Pydantic models define — {entityId, text} in, {entityId, vector} out. 
// The interesting part is what's not here: Python needed Field(alias="entityId") 
// because its internal convention is snake_case (entity_id) but the wire format is camelCase.
//  Java field names are already camelCase by convention, 
//  so Jackson (Spring Boot's default JSON library) serializes/deserializes entityId
//   correctly with zero annotations — the two languages' own naming conventions happen 
//   to line up with the wire format from opposite directions
```

## entity-upload-service/src/main/java/com/velocity/entityupload/model/NewsArticle.java

```java
package com.velocity.entityupload.model;

public record NewsArticle(
        String newsId,
        String category,
        String subcategory,
        String title,
        String abstractText) {

            //return the final text for embedding , which is the title and 
            // abstract concatenated together
    public String embeddingText() {
        String abstractPart = abstractText == null ? "" : abstractText;
        return (title + " " + abstractPart).trim();
    }
}

// =>2. Why String instead of Optional<String>?

// No conversion is needed.

// If you used Optional<String>, you'd have to wrap every value:

// Optional.ofNullable(dbValue)

// and unwrap it everywhere:

// article.abstractText().orElse("")
```

## entity-upload-service/src/main/java/com/velocity/entityupload/parser/MindNewsTsvParser.java

```java
package com.velocity.entityupload.parser;

import com.velocity.entityupload.model.NewsArticle;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.stream.Stream;

/**
 * Parses MIND's news.tsv: tab-separated, no header row.
 * Columns (per PROJECT_SPEC.md #0): News ID, Category, SubCategory, Title, Abstract,
 * URL, Title Entities, Abstract Entities. Only the first 5 are needed for embeddings;
 * parsing tolerates rows shorter than 8 columns since trailing columns can be truncated
 * in some MIND mirrors.
 */
public final class MindNewsTsvParser {
    
    private static final int MIN_REQUIRED_COLUMNS = 4; // News ID, Category, SubCategory, Title
    private static final int ABSTRACT_COLUMN_INDEX = 4;
// final class with a private constructor, all-static methods — 
// the standard Java idiom for a stateless utility class

// final class + private constructor + static methods
// public final class MindNewsTsvParser {
//     private MindNewsTsvParser() {}
// }
// Prevents creating objects (new MindNewsTsvParser()).
// Prevents inheritance.
// All methods are static because parsing doesn't need any object state.

// ➡️ Stateless utility class.

    private MindNewsTsvParser() {
    }

    public static Stream<NewsArticle> parse(Reader reader) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(reader);
        return bufferedReader.lines()
                .filter(line -> !line.isBlank())
                .map(MindNewsTsvParser::parseLine);
    }
// With Stream:


// Read one row
//       ↓
// Process it
//       ↓
// Read next row

// Memory stays low and downstream code can batch rows lazily.

// ➡️ Lazy processing and memory efficiency.

// Category, SubCategory, and Title are ever load-bearing for embeddings;
//  URL and the two Entities columns are never read at all by this milestone,
//   and Abstract is explicitly optional

    private static NewsArticle parseLine(String line) {
        String[] columns = line.split("\t", -1);
        if (columns.length < MIN_REQUIRED_COLUMNS) {
            throw new IllegalArgumentException(
                    "news.tsv row has fewer than " + MIN_REQUIRED_COLUMNS + " columns: " + line);
        }

        String newsId = columns[0];
        String category = columns[1];
        String subcategory = columns[2];
        String title = columns[3];
        String abstractText = columns.length > ABSTRACT_COLUMN_INDEX
                ? columns[ABSTRACT_COLUMN_INDEX]
                : null;

        return new NewsArticle(
                newsId,
                category,
                subcategory,
                title,
                (abstractText == null || abstractText.isBlank()) ? null : abstractText);
    }
}

// 2. Takes a Reader, not File/Path
// parse(Reader reader)

// Instead of:

// parse(File file)

// This separates responsibilities:

// Parser → parses text.
// IngestionService → opens/closes files.

// ➡️ Loose coupling and easy testing.



// TSV File
//     │
//     ▼
// MindNewsTsvParser
//     │
//     ▼
// NewsArticle
//     │
//     ▼
// IngestionService
//     │
//     ├──────────────► EmbeddingCreatorClient
//     │                      │
//     │                      ▼
//     │              HTTP POST /embed
//     │                      │
//     │                      ▼
//     │          Embedding Creator (Python FastAPI)
//     │                      │
//     │              Returns embedding vector
//     │                      ▼
//     └────────────── Receives response
//                            │
//                            ▼
//                   Save to PostgreSQL
```

## entity-upload-service/src/main/java/com/velocity/entityupload/repository/EntityRepository.java

```java
package com.velocity.entityupload.repository;

import com.velocity.entityupload.model.NewsArticle;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

// @Repository + constructor-injected JdbcTemplate
@Repository
public class EntityRepository {

    private static final String UPSERT_SQL = """
            INSERT INTO entities (entity_id, title, abstract, category, subcategory, vector)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (entity_id) DO UPDATE SET
                title = EXCLUDED.title,
                abstract = EXCLUDED.abstract,
                category = EXCLUDED.category,
                subcategory = EXCLUDED.subcategory,
                vector = EXCLUDED.vector
            """;

    private final JdbcTemplate jdbcTemplate;

    public EntityRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

//     With batching:

// Java
//    │
// 100 rows
//    │
//    ▼
// Database
    public void upsertBatch(List<EntityRow> rows) {
        jdbcTemplate.batchUpdate(UPSERT_SQL, rows, rows.size(), (ps, row) -> {
            ps.setString(1, row.article().newsId());
            ps.setString(2, row.article().title());
            ps.setString(3, row.article().abstractText());
            ps.setString(4, row.article().category());
            ps.setString(5, row.article().subcategory());
            ps.setBytes(6, row.vectorBytes());
        });
    }

    /**
     * Packs a float vector as little-endian float32 bytes — the contract Milestone 2's
     * Python LSH hasher reads back with {@code np.frombuffer(raw, dtype='<f4')}.
     */
    public static byte[] packVector(List<Double> vector) {
        ByteBuffer buffer = ByteBuffer.allocate(vector.size() * Float.BYTES)
                .order(ByteOrder.LITTLE_ENDIAN);
        for (Double value : vector) {
            buffer.putFloat(value.floatValue());
        }
        return buffer.array();
    }

    public record EntityRow(NewsArticle article, byte[] vectorBytes) {
    }
}

// 1. Why JdbcTemplate instead of JPA?

// Because this class has only one SQL query.

// INSERT ... ON CONFLICT DO UPDATE

// JdbcTemplate lets you execute that query directly:

// jdbcTemplate.batchUpdate(sql, ...)

// With JPA, you'd need:

// @Entity
// @Id
// JpaRepository
// understand save()/merge()

// That's a lot of setup just to run one SQL statement.

// Rule: If you already know the exact SQL, use JdbcTemplate.
```

## entity-upload-service/src/main/java/com/velocity/entityupload/service/IngestionService.java

```java
package com.velocity.entityupload.service;

import com.velocity.entityupload.client.EmbeddingCreatorClient;
import com.velocity.entityupload.dto.EmbedResponse;
import com.velocity.entityupload.model.NewsArticle;
import com.velocity.entityupload.parser.MindNewsTsvParser;
import com.velocity.entityupload.repository.EntityRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

// The orchestrator for Deliverable B: reads news.tsv, gets each article embedded by
// embedding-creator, and writes the results into Postgres. This is the only class that
// talks to both EmbeddingCreatorClient and EntityRepository — neither of those two talk
// to each other directly.
@Service
public class IngestionService {

    private static final Logger log = LoggerFactory.getLogger(IngestionService.class);

    private final EmbeddingCreatorClient embeddingCreatorClient;
    private final EntityRepository entityRepository;
    private final Path newsTsvPath;
    private final int batchSize;
    private final int embedConcurrency;

    public IngestionService(
            EmbeddingCreatorClient embeddingCreatorClient,
            EntityRepository entityRepository,
            @Value("${mind.news-tsv-path}") String newsTsvPath,
            @Value("${ingestion.batch-size}") int batchSize,
            @Value("${ingestion.embed-concurrency}") int embedConcurrency) {
        this.embeddingCreatorClient = embeddingCreatorClient;
        this.entityRepository = entityRepository;
        this.newsTsvPath = Path.of(newsTsvPath);
        this.batchSize = batchSize;
        this.embedConcurrency = embedConcurrency;
    }

    /**
     * Entry point, called by IngestController. Reads news.tsv (optionally capped at
     * {@code limit} rows), processes it in batches of {@code batchSize}, and returns a
     * summary of how many succeeded/failed.
     */
    public IngestionResult ingest(Integer limit) {
        // The thread pool that bounds how many /embed calls are in flight at once.
        // embedConcurrency (~6) is deliberately small: embedding-creator is one CPU-bound
        // Python process, so throwing more concurrent requests at it than that doesn't make
        // it faster, it just makes requests queue up on its side. This pool is created once
        // per /ingest call (not shared across calls) and is shut down in the `finally` block
        // below, whether ingestion succeeds or blows up.
        ExecutorService executor = Executors.newFixedThreadPool(embedConcurrency);

        // try-with-resources: both the file and the Stream get closed automatically,
        // even if an exception is thrown partway through.
        try (FileReader reader = new FileReader( newsTsvPath.toFile() );
             Stream<NewsArticle> articles = MindNewsTsvParser.parse(reader)) {

            // The parser gives us a lazy stream — nothing has been read from disk yet.
            // Optionally cap it at `limit` rows (used for fast verification runs, e.g.
            // POST /ingest?limit=20, instead of waiting on the full ~50k-row file).
            Stream<NewsArticle> bounded = limit != null ? articles.limit(limit) : articles;
            Iterator<NewsArticle> iterator = bounded.iterator();

            int total = 0;
            int succeeded = 0;
            List<String> failedIds = new ArrayList<>();
            List<NewsArticle> batch = new ArrayList<>(batchSize);

            // Pull articles out of the stream one at a time, accumulating them into
            // an in-memory batch. Once the batch hits batchSize (100), process it as a
            // unit (embed + write) and start a new empty batch.
            while (iterator.hasNext()) {
                batch.add(iterator.next());
                total++;
                if (batch.size() == batchSize) {
                    BatchOutcome outcome = processBatch(batch, executor);
                    succeeded += outcome.succeeded();
                    failedIds.addAll(outcome.failedIds());
                    log.info("Ingested {} articles so far", total);
                    batch.clear();
                }
            }
            // The file's row count won't always be a multiple of batchSize — process
            // whatever's left over in the final, possibly-smaller batch.
            if (!batch.isEmpty()) {
                BatchOutcome outcome = processBatch(batch, executor);
                succeeded += outcome.succeeded();
                failedIds.addAll(outcome.failedIds());
            }

            return new IngestionResult(total, succeeded, failedIds.size(), failedIds);
        } catch (IOException e) {
            // Can't read the file at all (bad path, permissions) — nothing to recover from,
            // so this is a hard failure of the whole /ingest call, not a per-row skip.
            throw new IllegalStateException("Failed to read MIND news.tsv at " + newsTsvPath, e);
        } finally {
            // Always release the thread pool's threads, whether ingest() returned
            // normally or an exception propagated out of the try block above.
            executor.shutdown();
        }
    }

    /**
     * Embeds every article in one batch (bounded concurrency via the shared executor),
     * then writes everything that succeeded to Postgres in a single upsert call.
     * One bad article does not fail the whole batch.
     */
    private BatchOutcome processBatch(List<NewsArticle> batch, ExecutorService executor) {
        // Submit all embed calls for this batch to the executor up front. Because the
        // pool only has `embedConcurrency` threads, at most that many of these actually
        // run at the same time — the rest queue until a thread frees up. Each submit()
        // returns immediately with a Future; nothing has necessarily finished yet.
        List<Future<EmbeddedArticle>> futures = new ArrayList<>(batch.size());
        for (NewsArticle article : batch) {
            futures.add(executor.submit(() -> embedOne(article)));
        }

        List<EntityRepository.EntityRow> rows = new ArrayList<>(batch.size());
        List<String> failedIds = new ArrayList<>();
        int succeeded = 0;

        // future.get() blocks until that specific article's embed call is done (or
        // already is done, if it finished while we were still submitting others).
        // Looping over the futures in submission order just means we wait for them in
        // that order — it does not force them to have run in that order.
        for (Future<EmbeddedArticle> future : futures) {
            try {
                EmbeddedArticle result = future.get();
                rows.add(new EntityRepository.EntityRow(result.article(), result.vectorBytes()));
                succeeded++;
            } catch (ExecutionException e) {
                // The Callable (embedOne) threw — future.get() wraps whatever it threw
                // inside ExecutionException, so the real error is in getCause().
                EmbedFailure failure = (EmbedFailure) e.getCause();
                log.warn("Embedding failed for entityId={}: {}",
                        failure.entityId(), failure.getCause().getMessage());
                // Deliberately NOT rethrown: one failed article is logged and skipped,
                // the rest of the batch still gets written.
                failedIds.add(failure.entityId());
            } catch (InterruptedException e) {
                // Someone interrupted this thread (e.g. app shutdown) while we were
                // waiting on future.get(). Restore the interrupt flag for callers further
                // up the stack, then abort — this is not a per-row failure, it's the
                // whole operation being cancelled.
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while ingesting batch", e);
            }
        }

        // One JDBC batch upsert for everything that succeeded in this batch, not one
        // write per row — this is the actual mechanism behind "batch size 100".
        if (!rows.isEmpty()) {
            entityRepository.upsertBatch(rows);
        }
        return new BatchOutcome(succeeded, failedIds);
    }

    /**
     * Embeds a single article by calling embedding-creator. Runs on one of the
     * executor's threads, so this method body is what actually executes concurrently.
     */
    private EmbeddedArticle embedOne(NewsArticle article) {
        try {
            // EmbeddingCreatorClient already retries once internally on transient
            // failure (see EmbeddingCreatorClient.MAX_ATTEMPTS) — if we get an exception
            // here, that retry already happened and failed too.
            EmbedResponse response =
                    embeddingCreatorClient.embed(article.newsId(), article.embeddingText());
            byte[] vectorBytes = EntityRepository.packVector(response.vector());
            return new EmbeddedArticle(article, vectorBytes);
        } catch (RuntimeException e) {
            // Wrap the real failure with which entityId caused it, so processBatch()
            // can log and skip the right article instead of just "something failed".
            throw new EmbedFailure(article.newsId(), e);
        }
    }

    // One article paired with its packed vector bytes, ready to become an EntityRow.
    private record EmbeddedArticle(NewsArticle article, byte[] vectorBytes) {
    }

    // Result of processing exactly one batch — how many of its articles made it into
    // Postgres, and the entityIds of the ones that didn't.
    private record BatchOutcome(int succeeded, List<String> failedIds) {
    }

    // Carries which entityId failed alongside the real underlying exception, so it can
    // travel through Future/ExecutionException and still be logged meaningfully.
    private static final class EmbedFailure extends RuntimeException {
        private final String entityId;

        private EmbedFailure(String entityId, Throwable cause) {
            super(cause);
            this.entityId = entityId;
        }

        private String entityId() {
            return entityId;
        }
    }

    // The response body of POST /ingest — a summary of the whole run, not per-batch.
    public record IngestionResult(int total, int succeeded, int failed, List<String> failedIds) {
    }
}

// TSV File
//    │
//    ▼
// Read articles
//    │
//    ▼
// Make batches (100)
//    │
//    ▼
// Ask Python to create embeddings
//    │
//    ▼
// Convert embeddings to bytes
//    │
//    ▼
// Store batch in PostgreSQL
//    │
//    ▼
// Repeat
//    │
//    ▼
// Return summary

// 3. Which one affects recommendation latency?
// ------------------------------------------------------------

// Neither.

// Recommendation latency depends on this:

// Recommendation Service
//         │
//         ▼
// Redis
// Neighbor Index

// It does **not** depend on whether the `/ingest` endpoint waited 8 minutes or returned immediately.

// Once ingestion has completed, both designs produce exactly the same stored data.
///////////////////////////////////
/// 
/// 
/// 
// both the services are entirely decoupled from each other, and the ingestion
//  service is a one-time batch job that runs in the background, while the 
// recommendation service is a real-time service that serves requests from users.
```

## entity-upload-service/src/main/resources/application.yml

```yaml
server:
  port: 8080

spring:
  application:
    name: entity-upload-service
  datasource:
    url: ${SPRING_DATASOURCE_URL}
    username: ${SPRING_DATASOURCE_USERNAME}
    password: ${SPRING_DATASOURCE_PASSWORD}

embedding-creator:
  base-url: ${EMBEDDING_CREATOR_URL}

mind:
  news-tsv-path: ${MIND_NEWS_TSV_PATH:/data/mind-small/train/news.tsv}

ingestion:
  batch-size: 100
  embed-concurrency: 6

```

## recommendation-service/Dockerfile

```
# --- Build stage ---
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace

COPY pom.xml .
RUN mvn -B dependency:go-offline

COPY src/ src/
RUN mvn -B clean package -DskipTests

# --- Runtime stage ---
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /workspace/target/recommendation-service-*.jar app.jar

EXPOSE 8080
CMD ["java", "-jar", "app.jar"]

```

## recommendation-service/pom.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>3.3.4</version>
        <relativePath/>
    </parent>

    <groupId>com.velocity</groupId>
    <artifactId>recommendation-service</artifactId>
    <version>0.1.0</version>
    <name>recommendation-service</name>
    <description>Milestone 5: GET /recommendations serving path — retrieval, merge, ranking, Bloom filter</description>

    <properties>
        <java.version>21</java.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-jdbc</artifactId>
        </dependency>
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
            <scope>runtime</scope>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-data-redis</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>

```

## recommendation-service/src/main/java/com/velocity/recommendation/RecommendationServiceApplication.java

```java
package com.velocity.recommendation;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class RecommendationServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(RecommendationServiceApplication.class, args);
    }
}

// redis fallback 
// if the redis is down, the signals can be read from Postgres 
// (entity_history.interaction_type) since they are already persisted there. 
// This ensures that the recommendation service can still function and provide 
// recommendations even if Redis is temporarily unavailable.

// entity_history.interaction_type already carries exactly this data, 
// written in the same batch as the Redis signal (per InteractionEventConsumer.consume()),
//  so nothing new needs to be built, just one more read path added to RecommendationService 
//  for the missing-signal case.

// redis fall back is not needed for the signals themselves, since they are already persisted in Postgres (entity_history.interaction_type) and can be read from there if Redis is down.
// signals computation 

```

## recommendation-service/src/main/java/com/velocity/recommendation/client/VectorHasherClient.java

```java
package com.velocity.recommendation.client;

import com.velocity.recommendation.dto.NeighborEntry;
import com.velocity.recommendation.dto.NeighborsResponse;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

import java.util.List;
import java.util.Optional;
// The VectorHasherClient ultimately reads from the neighbor_index_read table (through the vector-hasher service).

// That table is your read-optimized, mostly static cache/table built specifically for fast neighbor lookups.

// Single class responsible for calling vector-hasher's GET /neighbors/{entityId} — mirrors
// entity-upload-service's EmbeddingCreatorClient (RestClient injected via RestClientConfig, not
// built here). Reuses vector-hasher's own read-side cache and DB access rather than this service
// querying neighbor_index_read directly (see MILESTONE_5_PLAN.md decision 1).
public class VectorHasherClient {

    private final RestClient restClient;

    public VectorHasherClient(RestClient restClient) {
        this.restClient = restClient;
    }

    // Empty (not an exception) on 404 — an entity not yet present in neighbor_index_read just means
    // "skip this seed," not a request failure. Any other non-2xx still propagates as an exception.
    public Optional<List<NeighborEntry>> fetchNeighbors(String entityId) {
        try {
            NeighborsResponse response = restClient.get()
                    .uri("/neighbors/{entityId}", entityId)
                    .retrieve()
                    .body(NeighborsResponse.class);
            return Optional.ofNullable(response).map(NeighborsResponse::neighbors);
        } catch (HttpClientErrorException.NotFound e) {
            return Optional.empty();
        }
    }
}

```

## recommendation-service/src/main/java/com/velocity/recommendation/config/RestClientConfig.java

```java
package com.velocity.recommendation.config;

import com.velocity.recommendation.client.VectorHasherClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.ClientHttpRequestFactories;
import org.springframework.boot.web.client.ClientHttpRequestFactorySettings;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.time.Duration;

@Configuration
public class RestClientConfig {

    @Bean
    public RestClient vectorHasherRestClient(@Value("${vector-hasher.base-url}") String baseUrl) {
        ClientHttpRequestFactorySettings settings = ClientHttpRequestFactorySettings.DEFAULTS
                .withConnectTimeout(Duration.ofSeconds(5))
                .withReadTimeout(Duration.ofSeconds(10));
        ClientHttpRequestFactory requestFactory = ClientHttpRequestFactories.get(settings);

        return RestClient.builder()
                .baseUrl(baseUrl)
                .requestFactory(requestFactory)
                .build();
    }

    @Bean
    public VectorHasherClient vectorHasherClient(RestClient vectorHasherRestClient) {
        return new VectorHasherClient(vectorHasherRestClient);
    }
}
```

## recommendation-service/src/main/java/com/velocity/recommendation/controller/HealthController.java

```java
package com.velocity.recommendation.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HealthController {

    @GetMapping("/health")
    public String healthCheck() {
        return "Recommendation Service is healthy!";
    }
}

```

## recommendation-service/src/main/java/com/velocity/recommendation/controller/RecommendationController.java

```java
package com.velocity.recommendation.controller;

import com.velocity.recommendation.dto.RecommendationResponse;
import com.velocity.recommendation.service.RecommendationService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class RecommendationController {

    private final RecommendationService recommendationService;

    public RecommendationController(RecommendationService recommendationService) {
        this.recommendationService = recommendationService;
    }

    @GetMapping("/recommendations")
    public RecommendationResponse recommend(@RequestParam String userId) {
        return recommendationService.getRecommendations(userId);
    }
}

```

## recommendation-service/src/main/java/com/velocity/recommendation/dto/InteractionType.java

```java
package com.velocity.recommendation.dto;

// Duplicated from entity-interaction-service (no shared library between services) — must match
// entity_history.interaction_type (VARCHAR(10)) and Redis's user:{userId}:signals values exactly,
// since this service only ever reads values written by entity-interaction-service, never writes them.
public enum InteractionType {
    CLICK,
    LIKE,
    DISLIKE
}

```

## recommendation-service/src/main/java/com/velocity/recommendation/dto/NeighborEntry.java

```java
package com.velocity.recommendation.dto;

// Mirrors vector-hasher's GET /neighbors/{entityId} response shape: { neighbors: [{entityId, distance}, ...] }.
public record NeighborEntry(String entityId, double distance) {
}

```

## recommendation-service/src/main/java/com/velocity/recommendation/dto/NeighborsResponse.java

```java
package com.velocity.recommendation.dto;

import java.util.List;

// Mirrors vector-hasher's GET /neighbors/{entityId} response body exactly.
public record NeighborsResponse(String entityId, List<NeighborEntry> neighbors) {
}
// used in the VectorHasherClient to deserialize the response from the 
// vector-hasher service when fetching neighbors for a given entityId.
```

## recommendation-service/src/main/java/com/velocity/recommendation/dto/RecommendationItem.java

```java
package com.velocity.recommendation.dto;

// One entry in GET /recommendations's response body — matches PROJECT_SPEC.md §5.5's shape.
public record RecommendationItem(String entityId, String title, String category, double score) {
}

```

## recommendation-service/src/main/java/com/velocity/recommendation/dto/RecommendationResponse.java

```java
package com.velocity.recommendation.dto;

import java.util.List;

// GET /recommendations's full response body — matches PROJECT_SPEC.md §5.5's shape.
public record RecommendationResponse(String userId, List<RecommendationItem> recommendations) {
}

```

## recommendation-service/src/main/java/com/velocity/recommendation/repository/EntityHistoryLookupRepository.java

```java
package com.velocity.recommendation.repository;

import com.velocity.recommendation.dto.InteractionType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
// PostgreSQL fallback for user interaction history when Redis data is missing.

// Read-only access to entity_history (owned/written by entity-interaction-service) — the durable
// fallback behind all three Redis-state-loss gaps found while designing this service: last-N
// (step 1), the Bloom filter (step 5), and the signals hash (step 4). One consistent shape: check
// Redis existence first, fall back to a query here, let Redis self-heal on the user's next real
// interaction. Every query here is batched over a small (<=15, capped by NEIGHBOR_INDEX_K) set of
// ids, never a per-candidate round trip.
@Repository
public class EntityHistoryLookupRepository {

    private final JdbcTemplate jdbcTemplate;

    public EntityHistoryLookupRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    // Step 5: resolves the Bloom-positive-confirm set AND the Bloom-missing "confirm everyone" set
    // with the same query — a candidate is "seen" if it has any row at all for this user, regardless
    // of how many source_ids/interaction_types it has across repeats.
    public Set<String> findSeenEntityIds(String userId, List<String> candidateIds) {
        if (candidateIds.isEmpty()) {
            return Set.of();
        }
        String sql = "SELECT DISTINCT entity_id FROM entity_history WHERE user_id = ? AND entity_id = ANY(?)";
        List<String> seen = jdbcTemplate.query(sql,
                ps -> {
                    ps.setString(1, userId);
                    ps.setArray(2, ps.getConnection().createArrayOf("varchar", candidateIds.toArray()));
                },
                (rs, rowNum) -> rs.getString("entity_id"));
        return new HashSet<>(seen);
    }

    // Step 1: Redis's lastEntities list, rebuilt from the durable record. entity_history can hold
    // more than one row per entity_id (History + real-click repeats are intentionally not collapsed,
    // per PROJECT_SPEC.md §10), so this resolves the latest timestamp per distinct entity_id first,
    // then takes the most-recent `limit` of those — matching Redis's ring buffer semantics (each
    // entityId appears at most once), not just "the last `limit` rows".
    public List<String> findRecentEntityIds(String userId, int limit) {
        String sql = """
                SELECT entity_id FROM (
                    SELECT DISTINCT ON (entity_id) entity_id, event_timestamp
                    FROM entity_history
                    WHERE user_id = ?
                    ORDER BY entity_id, event_timestamp DESC
                ) AS latest_per_entity
                ORDER BY event_timestamp DESC
                LIMIT ?
                """;
        return jdbcTemplate.query(sql, (rs, rowNum) -> rs.getString("entity_id"), userId, limit);
    }

    // Step 4: the signals hash, rebuilt for just the (<=5) seed entityIds that need it. Same
    // "latest wins" resolution as findRecentEntityIds, for the same reason (repeats aren't collapsed
    // in entity_history the way they are in Redis).
    public Map<String, InteractionType> findLatestInteractionTypes(String userId, List<String> seedEntityIds) {
        if (seedEntityIds.isEmpty()) {
            return Map.of();
        }
        String sql = """
                SELECT DISTINCT ON (entity_id) entity_id, interaction_type
                FROM entity_history
                WHERE user_id = ? AND entity_id = ANY(?)
                ORDER BY entity_id, event_timestamp DESC
                """;
        Map<String, InteractionType> result = new HashMap<>();

        jdbcTemplate.query(sql,
                ps -> {
                    ps.setString(1, userId);
                    ps.setArray(2, ps.getConnection().createArrayOf("varchar", seedEntityIds.toArray()));
                },
                (RowCallbackHandler) rs ->
                        result.put(rs.getString("entity_id"), InteractionType.valueOf(rs.getString("interaction_type"))));
        return result;
    }
}
// The Bloom filter says: ===> The method runs:

// A → maybe seen              SELECT DISTINCT entity_id
// B → maybe seen                FROM entity_history
                    //   =====>      
// C → unseen                   WHERE user_id = ?
                            
// D → maybe seen               AND entity_id = ANY(?)
// where

// candidateIds = [A, B, D]
// Suppose the database contains:

// user1
// ------
// A
// D

// The query returns:

// [A, D]
```

## recommendation-service/src/main/java/com/velocity/recommendation/repository/EntityLookupRepository.java

```java
package com.velocity.recommendation.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
// This repository is not a Redis fallback.

// Its purpose is:

// Given a list of entityIds, fetch their metadata (title, category) from PostgreSQL.
// First SELECT-based repository in this codebase — every other repository so far (entity-upload
// -service's EntityRepository, entity-interaction-service's EntityHistoryRepository) is upsert-only.
@Repository
public class EntityLookupRepository {

    private static final String SELECT_SQL =
            "SELECT entity_id, title, category FROM entities WHERE entity_id = ANY(?)";

    private final JdbcTemplate jdbcTemplate;

    public EntityLookupRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<EntitySummary> findByIds(List<String> entityIds) {
        if (entityIds.isEmpty()) {
            return List.of();
        }
        return jdbcTemplate.query(SELECT_SQL,
                ps -> ps.setArray(1, ps.getConnection().createArrayOf("varchar", entityIds.toArray())),
                (rs, rowNum) -> new EntitySummary(
                        rs.getString("entity_id"), rs.getString("title"), rs.getString("category")));
    }

    public record EntitySummary(String entityId, String title, String category) {
    }
}

```

## recommendation-service/src/main/java/com/velocity/recommendation/service/BloomFilterReadService.java

```java
package com.velocity.recommendation.service;

import com.velocity.recommendation.util.MurmurHash3;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

// BloomFilterReadService is a read-only service that checks whether a user 
// has probably already seen a particular entity by reading that user's Bloom
//  filter stored in Redis.

// Its responsibilities are:

// Build the Redis key for the user's Bloom filter.
// Compute the 7 hash positions for a (userId, entityId) pair.
// Read those 7 bits from Redis.
// Return:
// false → definitely not seen.
// true → probably seen (Bloom filters can have false positives).

// Read-only mirror of entity-interaction-service's BloomFilterService (no add() — only that
// service's consumer ever writes these bits). BIT_SIZE/HASH_COUNT/the hash formula must match
// exactly, or the bit positions checked here wouldn't line up with the ones set on write.
@Service
public class BloomFilterReadService {
//     More hashes:

// ✅ Lower false positives (up to a point)
// user45|entity123
    private static final int BIT_SIZE = 960;
    private static final int HASH_COUNT = 7;

    private final StringRedisTemplate redisTemplate;

    public BloomFilterReadService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    // A missing key means "never built or wiped" — callers must not read that as "every bit is 0,
    // so nothing is seen." GETBIT on a missing key returns 0 for every position, indistinguishable
    // from a genuinely unseen entity, so this check has to happen before trusting mightContain at all.
    // Bloom filter exists, entity not seen ✅
    public boolean exists(String userId) {
        Boolean exists = redisTemplate.hasKey(key(userId));
        return Boolean.TRUE.equals(exists);
    }

    public boolean mightContain(String userId, String entityId) {
        String key = key(userId);
        for (int bitPosition : bitPositions(userId, entityId)) {
            if (!Boolean.TRUE.equals(redisTemplate.opsForValue().getBit(key, bitPosition))) {
                return false;
            }
        }
        return true;
    }

    private String key(String userId) {
        return "user:" + userId + ":bloomfilter";
    }

    private int[] bitPositions(String userId, String entityId) {
        byte[] data = (userId + "|" + entityId).getBytes(StandardCharsets.UTF_8);
        int h1 = MurmurHash3.hash32(data, 0);
        int h2 = MurmurHash3.hash32(data, 1);

        int[] positions = new int[HASH_COUNT];
        for (int i = 0; i < HASH_COUNT; i++) {
            positions[i] = Math.floorMod(h1 + i * h2, BIT_SIZE);
        }
        return positions;
    }
}

```

## recommendation-service/src/main/java/com/velocity/recommendation/service/CandidateRanker.java

```java
package com.velocity.recommendation.service;

import com.velocity.recommendation.dto.InteractionType;
import com.velocity.recommendation.dto.NeighborEntry;

import java.sql.Time;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Merge
// ↓
// Remove duplicates
// ↓
// Apply signal weights
// ↓
// Compute scores
// ↓
// Sort
// ↓
// Return recommendations
// Pure logic, no I/O: merges every seed's neighbor list into one deduped, ranked candidate list.
// Mirrors UserTimelineBuilder's role in entity-interaction-service — the one piece of real
// algorithmic complexity here, kept free of Redis/Postgres/HTTP so it's cheap to test exhaustively.
public final class CandidateRanker {

    // Placeholder ranking constants (same spirit as Milestone 4's 10%/5% LIKE/DISLIKE simulation
    // ratio) — deliberate, tunable demo choices, not derived values. Logged in PROJECT_SPEC.md §10.
    private static final double LIKE_DISTANCE_MULTIPLIER = 0.8;
    private static final double DISLIKE_DISTANCE_MULTIPLIER = 1.5;

    private CandidateRanker() {
    }

    /**
     * Seed A

        A
        ├── X
        ├── Y
        └── Z
        A -> LIKE

        B -> VIEW

        C -> DISLIKE
     * @param neighborsBySeed vector-hasher's response per seed entityId (seeds that 404'd are
     *                        simply absent, not included with an empty list)
     * @param signalsBySeed   each seed's own interaction signal (Redis, or the entity_history
     *                        fallback if Redis's signals hash was missing) — never the candidate's
     *                        own signal, since a candidate with its own signal would always also be
     *                        Bloom/history-"seen" and excluded downstream anyway
     * @return candidates deduped by entityId (min distance wins, ties broken by first-seen),
     * scored, sorted descending by score (best first)
     */
    public static List<RankedCandidate> rankCandidates(
            Map<String, List<NeighborEntry>> neighborsBySeed,
            Map<String, InteractionType> signalsBySeed) {

        Map<String, RankedCandidate> bestByEntityId = new HashMap<>();
//         Time Complexity

// Suppose

// S = number of seed entities.
// Each seed returns K neighbors.

// Then:

// Scanning & deduplicating: O(S × K) (one pass through all neighbors, with HashMap lookups).
// Sorting: If there are N unique candidates, O(N log N).

// Overall:

// O(S × K + N log N)

// Memory usage is O(N) for the bestByEntityId map.
        for (Map.Entry<String, List<NeighborEntry>> seedEntry : neighborsBySeed.entrySet()) {
            String seedId = seedEntry.getKey();
            for (NeighborEntry neighbor : seedEntry.getValue()) {
                RankedCandidate current = bestByEntityId.get(neighbor.entityId());
                // "Keep min distance" is decided on the raw distance, before any signal scaling —
                // the scaling reflects the winning seed's signal, it doesn't influence which seed wins.
                if (current == null || neighbor.distance() < current.distance()) {
                    double adjustedDistance = scale(neighbor.distance(), signalsBySeed.get(seedId));
                    double score = 1.0 / (1.0 + adjustedDistance);
                    bestByEntityId.put(neighbor.entityId(),
                            new RankedCandidate(neighbor.entityId(), seedId, neighbor.distance(), score));
                }
            }
        }

        return bestByEntityId.values().stream()
                .sorted(Comparator.comparingDouble(RankedCandidate::score).reversed())
                .toList();
    }

    private static double scale(double distance, InteractionType sourceSeedSignal) {
        if (sourceSeedSignal == InteractionType.LIKE) {
            return distance * LIKE_DISTANCE_MULTIPLIER;
        }
        if (sourceSeedSignal == InteractionType.DISLIKE) {
            return distance * DISLIKE_DISTANCE_MULTIPLIER;
        }
        return distance;
    }

    public record RankedCandidate(String entityId, String sourceSeedId, double distance, double score) {
    }
}
// Sure. In plain English, the loop does this:

// Take one seed entity (something the user previously interacted with).
// Look at all of its similar neighbors returned by the vector search.
// For each neighbor:
// Check if we've already seen this candidate.
// If we haven't, add it.
// If we have, keep whichever version has the smaller (better) vector distance.
// Use the seed's interaction type (LIKE, VIEW, or DISLIKE) to slightly adjust the candidate's distance.
// Convert that adjusted distance into a score (higher score = better recommendation).
// Store the candidate with its score.
// After all seeds have been processed, sort all unique candidates by score (highest first) and return the l
```

## recommendation-service/src/main/java/com/velocity/recommendation/service/RecommendationService.java

```java
package com.velocity.recommendation.service;

import com.velocity.recommendation.client.VectorHasherClient;
import com.velocity.recommendation.dto.InteractionType;
import com.velocity.recommendation.dto.NeighborEntry;
import com.velocity.recommendation.dto.RecommendationItem;
import com.velocity.recommendation.dto.RecommendationResponse;
import com.velocity.recommendation.repository.EntityHistoryLookupRepository;
import com.velocity.recommendation.repository.EntityLookupRepository;
import com.velocity.recommendation.repository.EntityLookupRepository.EntitySummary;
import com.velocity.recommendation.service.CandidateRanker.RankedCandidate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
// GET /recommendations/{userId}
//             │
//             ▼
// 1. Resolve seed entities
//    (Redis lastEntities → Postgres fallback)
//             │
//             ▼
// 2. Fetch neighbors for each seed
//    (parallel HTTP calls to vector-hasher)
//             │
//             ▼
// 3. Resolve interaction signals
//    (Redis signals → Postgres fallback)
//             │
//             ▼
// 4. Rank candidates
//    (CandidateRanker)
//             │
//             ▼
// 5. Remove already-seen entities
//    (Bloom filter + Postgres confirmation)
//             │
//             ▼
// 6. Keep Top-K recommendations
//             │
//             ▼
// 7. Fetch title/category
//    (EntityLookupRepository)
//             │
//             ▼
// 8. Return RecommendationResponse
// Orchestrates GET /recommendations: seeds (with Postgres fallback) -> parallel neighbor fetch ->
// merge/rank (CandidateRanker) -> Bloom-gated batched filter -> top-K -> entity lookup -> response.
// See MILESTONE_5_PLAN.md for the full reasoning behind each step.
@Service
public class RecommendationService {

    private final StringRedisTemplate redisTemplate;
    private final EntityHistoryLookupRepository entityHistoryLookupRepository;
    private final VectorHasherClient vectorHasherClient;
    private final BloomFilterReadService bloomFilterReadService;
    private final EntityLookupRepository entityLookupRepository;
    private final int lastNSize;
    private final int topK;

    public RecommendationService(
            StringRedisTemplate redisTemplate,
            EntityHistoryLookupRepository entityHistoryLookupRepository,
            VectorHasherClient vectorHasherClient,
            BloomFilterReadService bloomFilterReadService,
            EntityLookupRepository entityLookupRepository,
            @Value("${redis.last-n-size}") int lastNSize,
            @Value("${recommendation.top-k}") int topK) {
        this.redisTemplate = redisTemplate;
        this.entityHistoryLookupRepository = entityHistoryLookupRepository;
        this.vectorHasherClient = vectorHasherClient;
        this.bloomFilterReadService = bloomFilterReadService;
        this.entityLookupRepository = entityLookupRepository;
        this.lastNSize = lastNSize;
        this.topK = topK;
    }

    public RecommendationResponse getRecommendations(String userId) {
        List<String> seeds = resolveSeeds(userId);
        if (seeds.isEmpty()) {
            return new RecommendationResponse(userId, List.of());
        }

        Map<String, List<NeighborEntry>> neighborsBySeed = fetchNeighborsInParallel(seeds);
        Map<String, InteractionType> signalsBySeed = resolveSignals(userId, seeds);

        List<RankedCandidate> ranked = CandidateRanker.rankCandidates(neighborsBySeed, signalsBySeed);
        List<RankedCandidate> survivors = filterSeenAndTruncate(userId, ranked);

        return new RecommendationResponse(userId, buildItems(survivors));
    }

    // Step 1: an empty Redis result is ambiguous (cold-start vs. lost state), so it falls back to
    // the durable record rather than being read as "no history."
    private List<String> resolveSeeds(String userId) {
        List<String> lastEntities = redisTemplate.opsForList().range(lastEntitiesKey(userId), 0, -1);
        if (lastEntities != null && !lastEntities.isEmpty()) {
            return lastEntities;
        }
        return entityHistoryLookupRepository.findRecentEntityIds(userId, lastNSize);
    }

    // Step 2: one HTTP call per seed, run concurrently — at most lastNSize calls, cheap enough for
    // the default common pool rather than a dedicated executor.
    private Map<String, List<NeighborEntry>> fetchNeighborsInParallel(List<String> seeds) {
        Map<String, CompletableFuture<Optional<List<NeighborEntry>>>> futuresBySeed = new LinkedHashMap<>();
        for (String seedId : seeds) {
            futuresBySeed.put(seedId, CompletableFuture.supplyAsync(() -> vectorHasherClient.fetchNeighbors(seedId)));
        }
        CompletableFuture.allOf(futuresBySeed.values().toArray(new CompletableFuture[0])).join();

        Map<String, List<NeighborEntry>> neighborsBySeed = new LinkedHashMap<>();
        futuresBySeed.forEach((seedId, future) -> future.join().ifPresent(neighbors -> neighborsBySeed.put(seedId, neighbors)));
        return neighborsBySeed;
    }

    // Step 4: same existence-gated fallback shape as step 1 — a missing signals key means Redis
    // lost it (every processed entity gets one unconditionally), not that it never existed.
    private Map<String, InteractionType> resolveSignals(String userId, List<String> seeds) {
        String key = signalsKey(userId);
        if (!Boolean.TRUE.equals(redisTemplate.hasKey(key))) {
            return entityHistoryLookupRepository.findLatestInteractionTypes(userId, seeds);
        }

        HashOperations<String, String, String> hashOps = redisTemplate.opsForHash();
        List<String> values = hashOps.multiGet(key, seeds);
        Map<String, InteractionType> signalsBySeed = new LinkedHashMap<>();
        for (int i = 0; i < seeds.size(); i++) {
            String value = values.get(i);
            if (value != null) {
                signalsBySeed.put(seeds.get(i), InteractionType.valueOf(value));
            }
        }
        return signalsBySeed;
    }

    // Step 5: gate on the Bloom filter's own existence first (a missing key must not be read as
    // "every bit is 0, so nothing is seen"), then resolve every candidate that needs confirming with
    // one batched query — never a per-candidate round trip. Filtering the already-sorted `ranked`
    // list in a single pass (rather than splitting into separate free/needs-confirming lists and
    // recombining) is what keeps the final order correct without any extra re-sorting.
    private List<RankedCandidate> filterSeenAndTruncate(String userId, List<RankedCandidate> ranked) {
        boolean bloomTrustworthy = bloomFilterReadService.exists(userId);

        List<String> needsConfirming = new ArrayList<>();
        for (RankedCandidate candidate : ranked) {
            boolean possiblySeen = !bloomTrustworthy || bloomFilterReadService.mightContain(userId, candidate.entityId());
            if (possiblySeen) {
                needsConfirming.add(candidate.entityId());
            }
        }

        Set<String> trulySeen = entityHistoryLookupRepository.findSeenEntityIds(userId, needsConfirming);

        return ranked.stream()
                .filter(candidate -> !trulySeen.contains(candidate.entityId()))
                .limit(topK)
                .toList();
    }

    // Step 6: batch-fetch title/category for just the survivors, then reassemble in the survivors'
    // score order — EntityLookupRepository doesn't guarantee its result rows match input order.
    private List<RecommendationItem> buildItems(List<RankedCandidate> survivors) {
        if (survivors.isEmpty()) {
            return List.of();
        }
        List<String> survivorIds = survivors.stream().map(RankedCandidate::entityId).toList();
        Map<String, EntitySummary> summariesById = entityLookupRepository.findByIds(survivorIds).stream()
                .collect(Collectors.toMap(EntitySummary::entityId, summary -> summary));

        List<RecommendationItem> items = new ArrayList<>();
        for (RankedCandidate candidate : survivors) {
            EntitySummary summary = summariesById.get(candidate.entityId());
            if (summary != null) {
                items.add(new RecommendationItem(candidate.entityId(), summary.title(), summary.category(), candidate.score()));
            }
        }
        return items;
    }

    private String lastEntitiesKey(String userId) {
        return "user:" + userId + ":lastEntities";
    }

    private String signalsKey(String userId) {
        return "user:" + userId + ":signals";
    }
}

// Responsibilities of each step
// Step 1 — Resolve Seeds

// Input: userId

// Gets the user's recent entities.

// Prefer Redis (lastEntities)
// If Redis lost data → PostgreSQL fallback

// Output:

// [A, B, C, D, E]
// Step 2 — Fetch Neighbors

// For every seed:

// A → neighbors
// B → neighbors
// C → neighbors

// Uses parallel HTTP requests to the vector-hasher service.

// Output:

// Map<Seed, List<Neighbor>>
// Step 3 — Resolve Signals

// Gets whether each seed was

// LIKE
// VIEW
// DISLIKE

// Again:

// Redis first
// PostgreSQL fallback

// Output:

// A -> LIKE
// B -> VIEW
// C -> DISLIKE
// Step 4 — Rank Candidates

// Hands both maps to CandidateRanker.

// It:

// merges
// deduplicates
// scores
// sorts

// Output:

// RankedCandidate[]
// Step 5 — Filter Seen

// Removes recommendations the user already saw.

// Uses:

// Bloom Filter
//        +
// Postgres confirmation

// Output:

// Only unseen candidates
// Step 6 — Build Response

// Currently we only have IDs.

// Example:

// M123
// M456
// M789

// This step fetches

// Title
// Category

// and builds

// RecommendationItem

```

## recommendation-service/src/main/java/com/velocity/recommendation/util/MurmurHash3.java

```java
package com.velocity.recommendation.util;

// Hand-rolled MurmurHash3 x86-32 (Austin Appleby's public-domain algorithm) — duplicated verbatim
// from entity-interaction-service (no shared library between services, per project convention).
// Only call site here is BloomFilterReadService's mightContain check.
public final class MurmurHash3 {

    private static final int C1 = 0xcc9e2d51;
    private static final int C2 = 0x1b873593;

    private MurmurHash3() {
    }

    public static int hash32(byte[] data, int seed) {
        int hash = seed;
        int length = data.length;
        int blockCount = length / 4;

        for (int i = 0; i < blockCount; i++) {
            int k = (data[i * 4] & 0xff)
                    | ((data[i * 4 + 1] & 0xff) << 8)
                    | ((data[i * 4 + 2] & 0xff) << 16)
                    | ((data[i * 4 + 3] & 0xff) << 24);
            hash = mixBlock(hash, k);
        }

        int tailStart = blockCount * 4;
        int tail = 0;
        switch (length - tailStart) {
            case 3:
                tail ^= (data[tailStart + 2] & 0xff) << 16;
            case 2:
                tail ^= (data[tailStart + 1] & 0xff) << 8;
            case 1:
                tail ^= (data[tailStart] & 0xff);
                tail *= C1;
                tail = Integer.rotateLeft(tail, 15);
                tail *= C2;
                hash ^= tail;
        }

        hash ^= length;
        return finalize(hash);
    }

    private static int mixBlock(int hash, int k) {
        k *= C1;
        k = Integer.rotateLeft(k, 15);
        k *= C2;
        hash ^= k;
        hash = Integer.rotateLeft(hash, 13);
        return hash * 5 + 0xe6546b64;
    }

    private static int finalize(int h) {
        h ^= h >>> 16;
        h *= 0x85ebca6b;
        h ^= h >>> 13;
        h *= 0xc2b2ae35;
        h ^= h >>> 16;
        return h;
    }
}

```

## recommendation-service/src/main/resources/application.yml

```yaml
server:
  port: 8080

spring:
  application:
    name: recommendation-service
  datasource:
    url: ${SPRING_DATASOURCE_URL}
    username: ${SPRING_DATASOURCE_USERNAME}
    password: ${SPRING_DATASOURCE_PASSWORD}
  data:
    redis:
      host: ${REDIS_HOST}
      port: ${REDIS_PORT}

vector-hasher:
  base-url: ${VECTOR_HASHER_URL}

redis:
  last-n-size: ${LAST_N_SIZE}

recommendation:
  top-k: 10

```

## recommendation-service/src/test/java/com/velocity/recommendation/client/VectorHasherClientTest.java

```java
package com.velocity.recommendation.client;

import com.velocity.recommendation.dto.NeighborEntry;
import com.velocity.recommendation.dto.NeighborsResponse;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class VectorHasherClientTest {

    @SuppressWarnings("unchecked")
    @Test
    void fetchNeighborsReturnsTheNeighborListOnSuccess() {
        RestClient restClient = mock(RestClient.class);
        RestClient.RequestHeadersUriSpec uriSpec = mock(RestClient.RequestHeadersUriSpec.class);
        RestClient.ResponseSpec responseSpec = mock(RestClient.ResponseSpec.class);
        when(restClient.get()).thenReturn(uriSpec);
        when(uriSpec.uri("/neighbors/{entityId}", "N1")).thenReturn(uriSpec);
        when(uriSpec.retrieve()).thenReturn(responseSpec);
        when(responseSpec.body(NeighborsResponse.class))
                .thenReturn(new NeighborsResponse("N1", List.of(new NeighborEntry("N2", 0.12))));

        VectorHasherClient client = new VectorHasherClient(restClient);
        Optional<List<NeighborEntry>> result = client.fetchNeighbors("N1");

        assertThat(result).contains(List.of(new NeighborEntry("N2", 0.12)));
    }

    @SuppressWarnings("unchecked")
    @Test
    void fetchNeighborsReturnsEmptyOptionalOn404NotAnException() {
        RestClient restClient = mock(RestClient.class);
        RestClient.RequestHeadersUriSpec uriSpec = mock(RestClient.RequestHeadersUriSpec.class);
        RestClient.ResponseSpec responseSpec = mock(RestClient.ResponseSpec.class);
        when(restClient.get()).thenReturn(uriSpec);
        when(uriSpec.uri("/neighbors/{entityId}", "N-unknown")).thenReturn(uriSpec);
        when(uriSpec.retrieve()).thenReturn(responseSpec);
        when(responseSpec.body(NeighborsResponse.class)).thenThrow(
                HttpClientErrorException.create(HttpStatus.NOT_FOUND, "Not Found", HttpHeaders.EMPTY, new byte[0], null));

        VectorHasherClient client = new VectorHasherClient(restClient);

        assertThat(client.fetchNeighbors("N-unknown")).isEmpty();
    }

    @SuppressWarnings("unchecked")
    @Test
    void fetchNeighborsPropagatesOtherErrorsInsteadOfSwallowingThem() {
        RestClient restClient = mock(RestClient.class);
        RestClient.RequestHeadersUriSpec uriSpec = mock(RestClient.RequestHeadersUriSpec.class);
        RestClient.ResponseSpec responseSpec = mock(RestClient.ResponseSpec.class);
        when(restClient.get()).thenReturn(uriSpec);
        when(uriSpec.uri("/neighbors/{entityId}", "N1")).thenReturn(uriSpec);
        when(uriSpec.retrieve()).thenReturn(responseSpec);
        when(responseSpec.body(NeighborsResponse.class)).thenThrow(
                HttpClientErrorException.create(
                        HttpStatus.BAD_REQUEST, "Bad Request", HttpHeaders.EMPTY, new byte[0], null));

        VectorHasherClient client = new VectorHasherClient(restClient);

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> client.fetchNeighbors("N1"))
                .isInstanceOf(HttpClientErrorException.class);
    }
}

```

## recommendation-service/src/test/java/com/velocity/recommendation/controller/HealthControllerTest.java

```java
package com.velocity.recommendation.controller;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class HealthControllerTest {

    @Test
    void healthCheckReturnsAHealthyMessage() {
        assertThat(new HealthController().healthCheck()).isEqualTo("Recommendation Service is healthy!");
    }
}

```

## recommendation-service/src/test/java/com/velocity/recommendation/controller/RecommendationControllerTest.java

```java
package com.velocity.recommendation.controller;

import com.velocity.recommendation.dto.RecommendationResponse;
import com.velocity.recommendation.service.RecommendationService;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RecommendationControllerTest {

    @Test
    void delegatesToRecommendationServiceWithTheGivenUserId() {
        RecommendationService recommendationService = mock(RecommendationService.class);
        RecommendationResponse expected = new RecommendationResponse("U131", List.of());
        when(recommendationService.getRecommendations("U131")).thenReturn(expected);
        RecommendationController controller = new RecommendationController(recommendationService);

        RecommendationResponse result = controller.recommend("U131");

        assertThat(result).isEqualTo(expected);
    }
}

```

## recommendation-service/src/test/java/com/velocity/recommendation/repository/EntityHistoryLookupRepositoryTest.java

```java
package com.velocity.recommendation.repository;

import com.velocity.recommendation.dto.InteractionType;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class EntityHistoryLookupRepositoryTest {

    @Test
    void findSeenEntityIdsWithEmptyCandidatesNeverQueries() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        EntityHistoryLookupRepository repository = new EntityHistoryLookupRepository(jdbcTemplate);

        assertThat(repository.findSeenEntityIds("U1", List.of())).isEmpty();
        verifyNoInteractions(jdbcTemplate);
    }

    @SuppressWarnings("unchecked")
    @Test
    void findSeenEntityIdsBindsUserIdAndArrayAndCollectsResults() throws Exception {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        EntityHistoryLookupRepository repository = new EntityHistoryLookupRepository(jdbcTemplate);

        when(jdbcTemplate.query(anyString(), any(PreparedStatementSetter.class), any(RowMapper.class)))
                .thenAnswer(invocation -> {
                    RowMapper<String> rowMapper = invocation.getArgument(2);
                    ResultSet rs = mock(ResultSet.class);
                    when(rs.getString("entity_id")).thenReturn("N1");
                    return List.of(rowMapper.mapRow(rs, 0));
                });

        Set<String> result = repository.findSeenEntityIds("U1", List.of("N1", "N2"));

        assertThat(result).containsExactly("N1");

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<PreparedStatementSetter> pssCaptor = ArgumentCaptor.forClass(PreparedStatementSetter.class);
        verify(jdbcTemplate).query(sqlCaptor.capture(), pssCaptor.capture(), any(RowMapper.class));
        assertThat(sqlCaptor.getValue()).contains("SELECT DISTINCT entity_id FROM entity_history")
                .contains("WHERE user_id = ? AND entity_id = ANY(?)");

        PreparedStatement ps = mock(PreparedStatement.class);
        Connection connection = mock(Connection.class);
        Array array = mock(Array.class);
        when(ps.getConnection()).thenReturn(connection);
        when(connection.createArrayOf(eq("varchar"), any())).thenReturn(array);
        pssCaptor.getValue().setValues(ps);
        verify(ps).setString(1, "U1");
        verify(ps).setArray(2, array);
    }

    @Test
    void findRecentEntityIdsWithEmptyResultReturnsEmptyList() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        EntityHistoryLookupRepository repository = new EntityHistoryLookupRepository(jdbcTemplate);
        when(jdbcTemplate.query(anyString(), any(RowMapper.class), eq("U1"), eq(5))).thenReturn(List.of());

        List<String> result = repository.findRecentEntityIds("U1", 5);

        assertThat(result).isEmpty();
    }

    @Test
    void findRecentEntityIdsUsesDistinctOnPerEntityBeforeLimiting() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        EntityHistoryLookupRepository repository = new EntityHistoryLookupRepository(jdbcTemplate);
        when(jdbcTemplate.query(anyString(), any(RowMapper.class), eq("U1"), eq(5)))
                .thenReturn(List.of("N9", "N3"));

        List<String> result = repository.findRecentEntityIds("U1", 5);

        assertThat(result).containsExactly("N9", "N3");
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).query(sqlCaptor.capture(), any(RowMapper.class), eq("U1"), eq(5));
        assertThat(sqlCaptor.getValue()).contains("DISTINCT ON (entity_id)").contains("ORDER BY event_timestamp DESC");
    }

    @Test
    void findLatestInteractionTypesWithEmptySeedsNeverQueries() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        EntityHistoryLookupRepository repository = new EntityHistoryLookupRepository(jdbcTemplate);

        assertThat(repository.findLatestInteractionTypes("U1", List.of())).isEmpty();
        verifyNoInteractions(jdbcTemplate);
    }

    @SuppressWarnings("unchecked")
    @Test
    void findLatestInteractionTypesMapsEntityIdToInteractionType() throws Exception {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        EntityHistoryLookupRepository repository = new EntityHistoryLookupRepository(jdbcTemplate);

        doAnswer(invocation -> {
            RowCallbackHandler handler = invocation.getArgument(2);
            ResultSet rs = mock(ResultSet.class);
            when(rs.getString("entity_id")).thenReturn("N1");
            when(rs.getString("interaction_type")).thenReturn("LIKE");
            handler.processRow(rs);
            return null;
        }).when(jdbcTemplate).query(anyString(), any(PreparedStatementSetter.class), any(RowCallbackHandler.class));

        Map<String, InteractionType> result = repository.findLatestInteractionTypes("U1", List.of("N1"));

        assertThat(result).containsExactly(Map.entry("N1", InteractionType.LIKE));

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).query(sqlCaptor.capture(), any(PreparedStatementSetter.class), any(RowCallbackHandler.class));
        assertThat(sqlCaptor.getValue()).contains("DISTINCT ON (entity_id) entity_id, interaction_type");
    }
}

```

## recommendation-service/src/test/java/com/velocity/recommendation/repository/EntityLookupRepositoryTest.java

```java
package com.velocity.recommendation.repository;

import com.velocity.recommendation.repository.EntityLookupRepository.EntitySummary;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.RowMapper;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class EntityLookupRepositoryTest {

    @Test
    void emptyIdListNeverQueries() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        EntityLookupRepository repository = new EntityLookupRepository(jdbcTemplate);

        List<EntitySummary> result = repository.findByIds(List.of());

        assertThat(result).isEmpty();
        verifyNoInteractions(jdbcTemplate);
    }

    @SuppressWarnings("unchecked")
    @Test
    void findByIdsBindsAnArrayParameterAndMapsEveryColumn() throws Exception {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        EntityLookupRepository repository = new EntityLookupRepository(jdbcTemplate);
        List<String> ids = List.of("N1", "N2");

        repository.findByIds(ids);

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<PreparedStatementSetter> pssCaptor = ArgumentCaptor.forClass(PreparedStatementSetter.class);
        ArgumentCaptor<RowMapper<EntitySummary>> rowMapperCaptor = ArgumentCaptor.forClass(RowMapper.class);
        verify(jdbcTemplate).query(sqlCaptor.capture(), pssCaptor.capture(), rowMapperCaptor.capture());

        assertThat(sqlCaptor.getValue()).contains("SELECT entity_id, title, category FROM entities")
                .contains("WHERE entity_id = ANY(?)");

        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        Connection connection = mock(Connection.class);
        Array array = mock(Array.class);
        when(preparedStatement.getConnection()).thenReturn(connection);
        when(connection.createArrayOf(eq("varchar"), any())).thenReturn(array);

        pssCaptor.getValue().setValues(preparedStatement);

        ArgumentCaptor<Object[]> arrayContentsCaptor = ArgumentCaptor.forClass(Object[].class);
        verify(connection).createArrayOf(eq("varchar"), arrayContentsCaptor.capture());
        assertThat(arrayContentsCaptor.getValue()).containsExactly("N1", "N2");
        verify(preparedStatement).setArray(1, array);

        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getString("entity_id")).thenReturn("N1");
        when(resultSet.getString("title")).thenReturn("Some Title");
        when(resultSet.getString("category")).thenReturn("sports");

        EntitySummary mapped = rowMapperCaptor.getValue().mapRow(resultSet, 0);
        assertThat(mapped).isEqualTo(new EntitySummary("N1", "Some Title", "sports"));
    }
}

```

## recommendation-service/src/test/java/com/velocity/recommendation/service/BloomFilterReadServiceTest.java

```java
package com.velocity.recommendation.service;

import com.velocity.recommendation.util.MurmurHash3;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
// User ID + Entity ID
//         │
//         ▼
// Generate 7 hash positions
//         │
//         ▼
// Check those 7 bits in Redis
//         │
//         ├── Any bit = 0 ?
//         │      ▼
//         │    Definitely NOT seen
//         │
//         └── All bits = 1
//                ▼
//         Probably seen
class BloomFilterReadServiceTest {

    private static final int BIT_SIZE = 960;
    private static final int HASH_COUNT = 7;

    // Mirrors BloomFilterReadService's private bitPositions() formula, using the same public
    // MurmurHash3 utility, so tests can assert exact bit positions instead of "some bit got checked".
    private static int[] computeBitPositions(String userId, String entityId) {
        byte[] data = (userId + "|" + entityId).getBytes(StandardCharsets.UTF_8);
        int h1 = MurmurHash3.hash32(data, 0);
        int h2 = MurmurHash3.hash32(data, 1);
        int[] positions = new int[HASH_COUNT];
        for (int i = 0; i < HASH_COUNT; i++) {
            positions[i] = Math.floorMod(h1 + i * h2, BIT_SIZE);
        }
        return positions;
    }

    @Test
    void existsReturnsTrueWhenTheKeyIsPresent() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        when(redisTemplate.hasKey("user:U131:bloomfilter")).thenReturn(true);
        BloomFilterReadService service = new BloomFilterReadService(redisTemplate);

        assertThat(service.exists("U131")).isTrue();
    }

    @Test
    void existsReturnsFalseWhenTheKeyIsMissing() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        when(redisTemplate.hasKey("user:U131:bloomfilter")).thenReturn(false);
        BloomFilterReadService service = new BloomFilterReadService(redisTemplate);

        assertThat(service.exists("U131")).isFalse();
    }

    @Test
    void existsReturnsFalseWhenRedisReturnsNull() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        when(redisTemplate.hasKey("user:U131:bloomfilter")).thenReturn(null);
        BloomFilterReadService service = new BloomFilterReadService(redisTemplate);

        assertThat(service.exists("U131")).isFalse();
    }

    @Test
    void mightContainReturnsTrueWhenAllSevenBitsAreSet() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        ValueOperations<String, String> valueOps = mock(ValueOperations.class);
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(valueOps.getBit(anyString(), anyLong())).thenReturn(true);
        BloomFilterReadService service = new BloomFilterReadService(redisTemplate);

        assertThat(service.mightContain("U131", "N45")).isTrue();
    }

    @Test
    void mightContainChecksTheExactExpectedBitPositions() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        ValueOperations<String, String> valueOps = mock(ValueOperations.class);
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(valueOps.getBit(anyString(), anyLong())).thenReturn(true);
        BloomFilterReadService service = new BloomFilterReadService(redisTemplate);

        service.mightContain("U131", "N45");

        int[] expectedPositions = computeBitPositions("U131", "N45");
        for (int position : expectedPositions) {
            verify(valueOps).getBit("user:U131:bloomfilter", position);
        }
    }

    @Test
    void mightContainReturnsFalseAndShortCircuitsOnFirstUnsetBit() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        ValueOperations<String, String> valueOps = mock(ValueOperations.class);
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(valueOps.getBit(anyString(), anyLong())).thenReturn(false, true, true, true, true, true, true);
        BloomFilterReadService service = new BloomFilterReadService(redisTemplate);

        assertThat(service.mightContain("U131", "N45")).isFalse();
        verify(valueOps, times(1)).getBit(anyString(), anyLong());
    }

    @Test
    void mightContainTreatsNullBitAsUnset() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        ValueOperations<String, String> valueOps = mock(ValueOperations.class);
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(valueOps.getBit(anyString(), anyLong())).thenReturn(null);
        BloomFilterReadService service = new BloomFilterReadService(redisTemplate);

        assertThat(service.mightContain("U131", "N45")).isFalse();
    }
}

```

## recommendation-service/src/test/java/com/velocity/recommendation/service/CandidateRankerTest.java

```java
package com.velocity.recommendation.service;

import com.velocity.recommendation.dto.InteractionType;
import com.velocity.recommendation.dto.NeighborEntry;
import com.velocity.recommendation.service.CandidateRanker.RankedCandidate;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class CandidateRankerTest {

    @Test
    void mergesNonOverlappingNeighborsFromMultipleSeeds() {
        Map<String, List<NeighborEntry>> neighborsBySeed = Map.of(
                "SEED-A", List.of(new NeighborEntry("N1", 0.2)),
                "SEED-B", List.of(new NeighborEntry("N2", 0.3)));

        List<RankedCandidate> result = CandidateRanker.rankCandidates(neighborsBySeed, Map.of());

        assertThat(result).extracting(RankedCandidate::entityId).containsExactlyInAnyOrder("N1", "N2");
    }

    @Test
    void dedupesByEntityIdKeepingTheMinDistanceAndItsProvenance() {
        Map<String, List<NeighborEntry>> neighborsBySeed = Map.of(
                "SEED-A", List.of(new NeighborEntry("N1", 0.5)),
                "SEED-B", List.of(new NeighborEntry("N1", 0.2)));

        List<RankedCandidate> result = CandidateRanker.rankCandidates(neighborsBySeed, Map.of());

        assertThat(result).hasSize(1);
        assertThat(result.get(0).entityId()).isEqualTo("N1");
        assertThat(result.get(0).distance()).isEqualTo(0.2);
        assertThat(result.get(0).sourceSeedId()).isEqualTo("SEED-B");
    }

    @Test
    void likeSignalOnTheSourceSeedBoostsTheScore() {
        Map<String, List<NeighborEntry>> neighborsBySeed = Map.of("SEED-A", List.of(new NeighborEntry("N1", 0.5)));

        List<RankedCandidate> neutral = CandidateRanker.rankCandidates(neighborsBySeed, Map.of());
        List<RankedCandidate> liked = CandidateRanker.rankCandidates(
                neighborsBySeed, Map.of("SEED-A", InteractionType.LIKE));

        assertThat(liked.get(0).score()).isGreaterThan(neutral.get(0).score());
    }

    @Test
    void dislikeSignalOnTheSourceSeedPenalizesTheScore() {
        Map<String, List<NeighborEntry>> neighborsBySeed = Map.of("SEED-A", List.of(new NeighborEntry("N1", 0.5)));

        List<RankedCandidate> neutral = CandidateRanker.rankCandidates(neighborsBySeed, Map.of());
        List<RankedCandidate> disliked = CandidateRanker.rankCandidates(
                neighborsBySeed, Map.of("SEED-A", InteractionType.DISLIKE));

        assertThat(disliked.get(0).score()).isLessThan(neutral.get(0).score());
    }

    @Test
    void clickSignalIsTreatedAsNeutral() {
        Map<String, List<NeighborEntry>> neighborsBySeed = Map.of("SEED-A", List.of(new NeighborEntry("N1", 0.5)));

        List<RankedCandidate> noSignal = CandidateRanker.rankCandidates(neighborsBySeed, Map.of());
        List<RankedCandidate> clickSignal = CandidateRanker.rankCandidates(
                neighborsBySeed, Map.of("SEED-A", InteractionType.CLICK));

        assertThat(clickSignal.get(0).score()).isEqualTo(noSignal.get(0).score());
    }

    @Test
    void missingSignalEntryForASeedIsTreatedAsNeutral() {
        // SEED-A has no entry at all in signalsBySeed (e.g. it was never a real click, only History).
        Map<String, List<NeighborEntry>> neighborsBySeed = Map.of("SEED-A", List.of(new NeighborEntry("N1", 0.5)));

        List<RankedCandidate> result = CandidateRanker.rankCandidates(neighborsBySeed, Map.of("SEED-B", InteractionType.LIKE));

        // score = 1 / (1 + 0.5) = 0.6666...
        assertThat(result.get(0).score()).isCloseTo(1.0 / 1.5, within(1e-9));
    }

    @Test
    void resultsAreSortedByScoreDescending() {
        Map<String, List<NeighborEntry>> neighborsBySeed = Map.of(
                "SEED-A", List.of(new NeighborEntry("FAR", 0.9), new NeighborEntry("CLOSE", 0.1)));

        List<RankedCandidate> result = CandidateRanker.rankCandidates(neighborsBySeed, Map.of());

        assertThat(result).extracting(RankedCandidate::entityId).containsExactly("CLOSE", "FAR");
    }

    @Test
    void emptyNeighborsProduceEmptyResult() {
        assertThat(CandidateRanker.rankCandidates(Map.of(), Map.of())).isEmpty();
    }

    @Test
    void minDistanceWinsProvenanceEvenWhenAFartherSeedWasLiked() {
        // Documented simplification: a candidate referenced by a closer neutral seed AND a farther
        // LIKE seed uses the closer/neutral seed's provenance — no boost applied in this case.
        Map<String, List<NeighborEntry>> neighborsBySeed = Map.of(
                "NEUTRAL-CLOSER", List.of(new NeighborEntry("N1", 0.2)),
                "LIKED-FARTHER", List.of(new NeighborEntry("N1", 0.6)));
        Map<String, InteractionType> signals = Map.of("LIKED-FARTHER", InteractionType.LIKE);

        List<RankedCandidate> result = CandidateRanker.rankCandidates(neighborsBySeed, signals);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).sourceSeedId()).isEqualTo("NEUTRAL-CLOSER");
        // No LIKE boost applied, since the winning (closer) seed was neutral: score = 1 / (1 + 0.2).
        assertThat(result.get(0).score()).isCloseTo(1.0 / 1.2, within(1e-9));
    }
}

```

## recommendation-service/src/test/java/com/velocity/recommendation/service/RecommendationServiceTest.java

```java
package com.velocity.recommendation.service;

import com.velocity.recommendation.client.VectorHasherClient;
import com.velocity.recommendation.dto.InteractionType;
import com.velocity.recommendation.dto.NeighborEntry;
import com.velocity.recommendation.dto.RecommendationItem;
import com.velocity.recommendation.dto.RecommendationResponse;
import com.velocity.recommendation.repository.EntityHistoryLookupRepository;
import com.velocity.recommendation.repository.EntityLookupRepository;
import com.velocity.recommendation.repository.EntityLookupRepository.EntitySummary;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.mockito.Mockito.*;

class RecommendationServiceTest {

    private final StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
    private final ListOperations<String, String> listOps = mock(ListOperations.class);
    private final HashOperations<String, String, String> hashOps = mock(HashOperations.class);
    private final EntityHistoryLookupRepository entityHistoryLookupRepository = mock(EntityHistoryLookupRepository.class);
    private final VectorHasherClient vectorHasherClient = mock(VectorHasherClient.class);
    private final BloomFilterReadService bloomFilterReadService = mock(BloomFilterReadService.class);
    private final EntityLookupRepository entityLookupRepository = mock(EntityLookupRepository.class);

    private RecommendationService newService(int lastNSize, int topK) {
        when(redisTemplate.opsForList()).thenReturn(listOps);
        when(redisTemplate.<String, String>opsForHash()).thenReturn(hashOps);
        return new RecommendationService(redisTemplate, entityHistoryLookupRepository, vectorHasherClient,
                bloomFilterReadService, entityLookupRepository, lastNSize, topK);
    }

    @Test
    void coldStartUserWithNoHistoryAnywhereReturnsEmptyAndShortCircuits() {
        when(listOps.range("user:U1:lastEntities", 0, -1)).thenReturn(List.of());
        when(entityHistoryLookupRepository.findRecentEntityIds("U1", 5)).thenReturn(List.of());
        RecommendationService service = newService(5, 10);

        RecommendationResponse response = service.getRecommendations("U1");

        assertThat(response).isEqualTo(new RecommendationResponse("U1", List.of()));
        verifyNoInteractions(vectorHasherClient, bloomFilterReadService, entityLookupRepository);
    }

    @Test
    void emptyRedisLastNFallsBackToPostgresRecentEntities() {
        when(listOps.range("user:U1:lastEntities", 0, -1)).thenReturn(List.of());
        when(entityHistoryLookupRepository.findRecentEntityIds("U1", 5)).thenReturn(List.of("SEED-A"));
        when(vectorHasherClient.fetchNeighbors("SEED-A")).thenReturn(Optional.of(List.of(new NeighborEntry("N1", 0.2))));
        when(redisTemplate.hasKey("user:U1:signals")).thenReturn(false);
        when(entityHistoryLookupRepository.findLatestInteractionTypes("U1", List.of("SEED-A"))).thenReturn(Map.of());
        when(bloomFilterReadService.exists("U1")).thenReturn(true);
        when(bloomFilterReadService.mightContain("U1", "N1")).thenReturn(false);
        when(entityHistoryLookupRepository.findSeenEntityIds("U1", List.of())).thenReturn(Set.of());
        when(entityLookupRepository.findByIds(List.of("N1")))
                .thenReturn(List.of(new EntitySummary("N1", "Title 1", "sports")));
        RecommendationService service = newService(5, 10);

        RecommendationResponse response = service.getRecommendations("U1");

        verify(entityHistoryLookupRepository).findRecentEntityIds("U1", 5);
        assertThat(response.recommendations()).extracting(RecommendationItem::entityId).containsExactly("N1");
    }

    @Test
    void signalsKeyMissingFallsBackToPostgresInsteadOfHashOps() {
        when(listOps.range("user:U1:lastEntities", 0, -1)).thenReturn(List.of("SEED-A"));
        when(vectorHasherClient.fetchNeighbors("SEED-A")).thenReturn(Optional.of(List.of(new NeighborEntry("N1", 0.5))));
        when(redisTemplate.hasKey("user:U1:signals")).thenReturn(false);
        when(entityHistoryLookupRepository.findLatestInteractionTypes("U1", List.of("SEED-A")))
                .thenReturn(Map.of("SEED-A", InteractionType.LIKE));
        when(bloomFilterReadService.exists("U1")).thenReturn(true);
        when(bloomFilterReadService.mightContain("U1", "N1")).thenReturn(false);
        when(entityHistoryLookupRepository.findSeenEntityIds("U1", List.of())).thenReturn(Set.of());
        when(entityLookupRepository.findByIds(List.of("N1")))
                .thenReturn(List.of(new EntitySummary("N1", "Title 1", "sports")));
        RecommendationService service = newService(5, 10);

        RecommendationResponse response = service.getRecommendations("U1");

        verifyNoInteractions(hashOps);
        verify(entityHistoryLookupRepository).findLatestInteractionTypes("U1", List.of("SEED-A"));
        // LIKE scaling applied: distance 0.5 * 0.8 = 0.4 -> score = 1/1.4
        assertThat(response.recommendations().get(0).score()).isCloseTo(1.0 / 1.4, within(1e-9));
    }

    @Test
    void bloomFilterKeyMissingConfirmsEveryCandidateNotJustBloomPositives() {
        when(listOps.range("user:U1:lastEntities", 0, -1)).thenReturn(List.of("SEED-A"));
        when(vectorHasherClient.fetchNeighbors("SEED-A"))
                .thenReturn(Optional.of(List.of(new NeighborEntry("N1", 0.2), new NeighborEntry("N2", 0.3))));
        when(redisTemplate.hasKey("user:U1:signals")).thenReturn(true);
        when(hashOps.multiGet("user:U1:signals", List.of("SEED-A"))).thenReturn(java.util.Collections.singletonList(null));
        when(bloomFilterReadService.exists("U1")).thenReturn(false);
        when(entityHistoryLookupRepository.findSeenEntityIds(eq("U1"), any())).thenReturn(Set.of());
        when(entityLookupRepository.findByIds(any())).thenReturn(List.of(
                new EntitySummary("N1", "Title 1", "sports"), new EntitySummary("N2", "Title 2", "news")));
        RecommendationService service = newService(5, 10);

        service.getRecommendations("U1");

        verify(bloomFilterReadService, never()).mightContain(anyString(), anyString());
        verify(entityHistoryLookupRepository).findSeenEntityIds(eq("U1"), argThat(ids ->
                ids.containsAll(List.of("N1", "N2")) && ids.size() == 2));
    }

    @Test
    void aFourOhFourFromVectorHasherSkipsThatSeedWithoutFailingTheRequest() {
        when(listOps.range("user:U1:lastEntities", 0, -1)).thenReturn(List.of("SEED-A", "SEED-B"));
        when(vectorHasherClient.fetchNeighbors("SEED-A")).thenReturn(Optional.empty());
        when(vectorHasherClient.fetchNeighbors("SEED-B")).thenReturn(Optional.of(List.of(new NeighborEntry("N1", 0.3))));
        when(redisTemplate.hasKey("user:U1:signals")).thenReturn(false);
        when(entityHistoryLookupRepository.findLatestInteractionTypes(eq("U1"), any())).thenReturn(Map.of());
        when(bloomFilterReadService.exists("U1")).thenReturn(true);
        when(bloomFilterReadService.mightContain("U1", "N1")).thenReturn(false);
        when(entityHistoryLookupRepository.findSeenEntityIds("U1", List.of())).thenReturn(Set.of());
        when(entityLookupRepository.findByIds(List.of("N1")))
                .thenReturn(List.of(new EntitySummary("N1", "Title 1", "sports")));
        RecommendationService service = newService(5, 10);

        RecommendationResponse response = service.getRecommendations("U1");

        assertThat(response.recommendations()).extracting(RecommendationItem::entityId).containsExactly("N1");
    }

    @Test
    void survivorsAreTruncatedToTopKAfterSeenFilteringAndKeepScoreOrder() {
        when(listOps.range("user:U1:lastEntities", 0, -1)).thenReturn(List.of("SEED-A"));
        when(vectorHasherClient.fetchNeighbors("SEED-A")).thenReturn(Optional.of(List.of(
                new NeighborEntry("CLOSE", 0.1), new NeighborEntry("MID", 0.5), new NeighborEntry("FAR", 0.9))));
        when(redisTemplate.hasKey("user:U1:signals")).thenReturn(false);
        when(entityHistoryLookupRepository.findLatestInteractionTypes(eq("U1"), any())).thenReturn(Map.of());
        when(bloomFilterReadService.exists("U1")).thenReturn(true);
        when(bloomFilterReadService.mightContain(eq("U1"), anyString())).thenReturn(false);
        when(entityHistoryLookupRepository.findSeenEntityIds("U1", List.of())).thenReturn(Set.of());
        when(entityLookupRepository.findByIds(any())).thenReturn(List.of(
                new EntitySummary("CLOSE", "Close Title", "c"),
                new EntitySummary("MID", "Mid Title", "c")));
        RecommendationService service = newService(5, 2);

        RecommendationResponse response = service.getRecommendations("U1");

        assertThat(response.recommendations()).extracting(RecommendationItem::entityId)
                .containsExactly("CLOSE", "MID");
    }

    @Test
    void finalResponseOrderMatchesScoreOrderNotEntityLookupReturnOrder() {
        when(listOps.range("user:U1:lastEntities", 0, -1)).thenReturn(List.of("SEED-A", "SEED-B"));
        when(vectorHasherClient.fetchNeighbors("SEED-A")).thenReturn(Optional.of(List.of(
                new NeighborEntry("N1", 0.2), new NeighborEntry("N2", 0.5))));
        when(vectorHasherClient.fetchNeighbors("SEED-B")).thenReturn(Optional.of(List.of(new NeighborEntry("N3", 0.3))));
        when(redisTemplate.hasKey("user:U1:signals")).thenReturn(true);
        when(hashOps.multiGet("user:U1:signals", List.of("SEED-A", "SEED-B")))
                .thenReturn(List.of("LIKE", "CLICK"));
        when(bloomFilterReadService.exists("U1")).thenReturn(true);
        when(bloomFilterReadService.mightContain("U1", "N1")).thenReturn(false);
        when(bloomFilterReadService.mightContain("U1", "N2")).thenReturn(true);
        when(bloomFilterReadService.mightContain("U1", "N3")).thenReturn(true);
        // N2 is truly seen (excluded); N3 is a Bloom false positive (kept).
        when(entityHistoryLookupRepository.findSeenEntityIds(eq("U1"), argThat(ids ->
                ids.containsAll(List.of("N2", "N3")) && ids.size() == 2)))
                .thenReturn(Set.of("N2"));
        // Deliberately returned in the opposite order of the expected score ranking.
        when(entityLookupRepository.findByIds(any())).thenReturn(List.of(
                new EntitySummary("N3", "Title 3", "news"),
                new EntitySummary("N1", "Title 1", "sports")));
        RecommendationService service = newService(5, 10);

        RecommendationResponse response = service.getRecommendations("U1");

        // N1: dist 0.2, LIKE seed -> adjusted 0.16 -> score ~0.862
        // N3: dist 0.3, neutral (CLICK) seed -> score ~0.769
        // N2 excluded (truly seen).
        assertThat(response.recommendations()).extracting(RecommendationItem::entityId).containsExactly("N1", "N3");
    }
}

```

## recommendation-service/src/test/java/com/velocity/recommendation/util/MurmurHash3Test.java

```java
package com.velocity.recommendation.util;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
// Used for the Bloom filter check in step 5 — 
// BloomFilterReadService.mightContain(userId, entityId).

// To check "has this user probably seen this article," 
// recommendation-service needs to hash (userId, entityId)
//  into the same 7 bit-positions that entity-interaction-service 
// set when writing that Bloom filter — so it must use the exact 
// same MurmurHash3 implementation, or the bit positions wouldn't 
// match and the check would be meaningless. Since there's no shared 
// library between services, MurmurHash3.java is duplicated verbatim
//  into recommendation-service, read-only (GETBIT only, never SETBIT).

// Not used for LIKE/DISLIKE — that stays exclusively in entity-interaction-service's 
// classify(), as we just settled.


class MurmurHash3Test {

    @Test
    void sameInputAndSeedAlwaysProduceSameHash() {
        byte[] data = "U131|N12345".getBytes(StandardCharsets.UTF_8);

        int first = MurmurHash3.hash32(data, 0);
        int second = MurmurHash3.hash32(data, 0);

        assertThat(first).isEqualTo(second);
    }

    @Test
    void differentSeedsProduceDifferentHashesForSameInput() {
        byte[] data = "U131|N12345".getBytes(StandardCharsets.UTF_8);

        int seed0 = MurmurHash3.hash32(data, 0);
        int seed1 = MurmurHash3.hash32(data, 1);

        assertThat(seed0).isNotEqualTo(seed1);
    }

    @Test
    void emptyInputWithZeroSeedHashesToZero() {
        // Derived directly from the algorithm: with no blocks, no tail bytes, and length 0,
        // hash stays 0 through every mixing step, so finalize(0) == 0.
        assertThat(MurmurHash3.hash32(new byte[0], 0)).isZero();
    }

    @Test
    void handlesTailLengthsOfOneTwoAndThreeBytesWithoutError() {
        for (int length = 1; length <= 3; length++) {
            byte[] data = new byte[length];
            for (int i = 0; i < length; i++) {
                data[i] = (byte) (i + 1);
            }
            int hash = MurmurHash3.hash32(data, 0);
            int hashAgain = MurmurHash3.hash32(data, 0);
            assertThat(hash).isEqualTo(hashAgain);
        }
    }

    @Test
    void changingOneByteChangesTheHash() {
        byte[] data = "N12345".getBytes(StandardCharsets.UTF_8);
        byte[] mutated = "N12346".getBytes(StandardCharsets.UTF_8);

        assertThat(MurmurHash3.hash32(data, 0)).isNotEqualTo(MurmurHash3.hash32(mutated, 0));
    }
}

```

## vector-hasher/Dockerfile

```
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ app/

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

```

## vector-hasher/app/__init__.py

```python

```

## vector-hasher/app/cache.py

```python
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
```

## vector-hasher/app/db.py

```python
from html import entities

import numpy as np
import psycopg
from psycopg.types.json import Jsonb

# to get all the vectors from the database , to compute the shard ids 
# for each vector and then update the shard ids in the database.
def fetch_all_vectors(dsn: str) -> list[tuple[str, np.ndarray]]:
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT entity_id, vector FROM entities")
            rows = cur.fetchall()
    return [ (entity_id, np.frombuffer(raw, dtype="<f4")) for entity_id, raw in rows]

# Each entity contributes 2 SQL parameters (e.g., entity_id and shard_id).

# So:

# 65,239 entities × 2 parameters
# = 130,478 parameters

# PostgreSQL has a hard limit of 65,535 bind parameters per statement, so the query fails once you exceed that limit.
# assingning shardIDs to each vectoe in the database , and add it in database .
# Batched: 2 params per assignment, and Postgres has a hard 65,535-param-per-query limit — a
# single UPDATE for the whole dataset works fine at Milestone 2/3's ~20-entity test scale but
# breaks past ~32,767 assignments. 10,000 per batch (20,000 params) stays well under that limit.
_SHARD_UPDATE_BATCH_SIZE = 10_000


def update_shard_ids(dsn: str, assignments: list[tuple[str, int]]) -> None:
    if not assignments:
        return

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            for start in range(0, len(assignments), _SHARD_UPDATE_BATCH_SIZE):
                batch = assignments[start:start + _SHARD_UPDATE_BATCH_SIZE]
                values_clause = ", ".join(["(%s::varchar, %s::integer)"] * len(batch))
                params = [value for pair in batch for value in pair]
                query = f"""
                    UPDATE entities AS e
                    SET shard_id = data.shard_id
                    FROM (VALUES {values_clause}) AS data(entity_id, shard_id)
                    WHERE e.entity_id = data.entity_id
                """
                cur.execute(query, params)

# // Fetch the shard ID for a given entity.
def fetch_shard_id(dsn: str, entity_id: str) -> int | None:
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT shard_id FROM entities WHERE entity_id = %s", (entity_id,)
            )
            row = cur.fetchone()
    return row[0] if row else None

# // // Fetch the embedding vector for a given entity.
def fetch_vector(dsn: str, entity_id: str) -> np.ndarray | None:
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT vector FROM entities WHERE entity_id = %s", (entity_id,)
            )
            row = cur.fetchone()
    if row is None:
        return None
    return np.frombuffer(row[0], dtype="<f4")

# /// get the similar vectors from the same shard and adjacent shards , excluding the current entity id.
def fetch_candidates(
    dsn: str, shard_ids: list[int], exclude_entity_id: str
) -> list[dict[str, str]]:
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT entity_id, title FROM entities
                WHERE shard_id = ANY(%s) AND entity_id != %s
                """,
                (shard_ids, exclude_entity_id),
            )
            rows = cur.fetchall()
    return [{"entityId": entity_id, "title": title} for entity_id, title in rows]

# ///////////returns the vectors , from the same shard and adjacent shards , excluding the current entity id.
def fetch_candidates_with_vectors(
    dsn: str, shard_ids: list[int], exclude_entity_id: str
) -> list[tuple[str, np.ndarray]]:
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT entity_id, vector FROM entities
                WHERE shard_id = ANY(%s) AND entity_id != %s
                """,
                (shard_ids, exclude_entity_id),
            )
            rows = cur.fetchall()
    return [(entity_id, np.frombuffer(raw, dtype="<f4")) for entity_id, raw in rows]

# Retrieve the precomputed neighbors for a given entity from the database.
def fetch_neighbors_write(dsn: str, entity_id: str) -> list[dict[str, object]] | None:
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT neighbors FROM neighbor_index_write WHERE entity_id = %s",
                (entity_id,),
            )
            row = cur.fetchone()
    return row[0] if row else None
# OUTPUT:
# row = (
#     [
#         {"entityId":"B2","distance":0.12},
#         {"entityId":"C9","distance":0.21}
#     ],
# )

# Save the computed neighbors of one entity into PostgreSQL.
# neighbors = [
#     (0.15, "A21"),
#     (0.27, "B44"),
#     (0.42, "C90")
# ]
def save_neighbors(
    dsn: str, entity_id: str, shard_id: int, neighbors: list[tuple[float, str]]
) -> None:
    neighbors_json = [
        {"entityId": neighbor_id, "distance": distance}
        for distance, neighbor_id in neighbors
    ]
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO neighbor_index_write (entity_id, shard_id, neighbors)
                VALUES (%s, %s, %s)
                ON CONFLICT (entity_id) DO UPDATE
                SET shard_id = EXCLUDED.shard_id,
                    neighbors = EXCLUDED.neighbors,
                    updated_at = now()
                """,
                (entity_id, shard_id, Jsonb(neighbors_json)),
            )



# Called by the Kafka consumer only. Unlike save_neighbors, the neighbors
# list here already arrived as JSON off the Kafka message — it's already
# shaped as [{"entityId": ..., "distance": ...}, ...], so no tuple-to-dict
# conversion is needed here, just wrap and store as-is.
def save_neighbors_read(
    dsn: str, entity_id: str, neighbors: list[dict[str, object]]
) -> None:
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO neighbor_index_read (entity_id, neighbors)
                VALUES (%s, %s)
                ON CONFLICT (entity_id) DO UPDATE
                SET neighbors = EXCLUDED.neighbors,
                    updated_at = now()
                """,
                (entity_id, Jsonb(neighbors)),
            )


def fetch_neighbors_read(dsn: str, entity_id: str) -> list[dict[str, object]] | None:
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT neighbors FROM neighbor_index_read WHERE entity_id = %s",
                (entity_id,),
            )
            row = cur.fetchone()
    return row[0] if row else None

# ### `fetch_all_vectors()`

# Reads every `entity_id` and embedding vector from the `entities` table.  
# `np.frombuffer(raw, dtype="<f4")` directly converts the stored `BYTEA` bytes into a 384-dimensional `float32` NumPy array. It is the Python counterpart of Java's `EntityRepository.packVector()`.

# ---

# ### `update_shard_ids()`

# Bulk-updates the computed `shard_id` values in a **single SQL query** using `UPDATE ... FROM (VALUES ...)`, avoiding one `UPDATE` per row. Explicit `::varchar` and `::integer` casts ensure PostgreSQL correctly infers parameter types.

# ---

# ### `fetch_shard_id()`

# Fetches the current `shard_id` for a given `entity_id`. Used to determine which shard(s) should be searched for recommendations.

# ---

# ### `fetch_candidates()`

# Retrieves articles whose `shard_id` belongs to the given list (e.g., current and adjacent shards), excluding the current article. `psycopg` automatically converts the Python `list[int]` into a PostgreSQL array for the `ANY(...)` clause.

# ============================================================
# HOW "SIMILAR VECTORS" ACTUALLY GET FOUND — FLOW MAP
# ============================================================
#
#   Milestone 2 (already done)
#   ┌───────────────────────────────────────────────┐
#   │ Every entity already has a shard_id saved.     │
#   │ Similar vectors USUALLY (not always) share     │
#   │ the same shard_id. Not guaranteed — see        │
#   │ resolve_candidate_shard_ids's adjacent-shard    │
#   │ fallback in lsh.py for the "not always" case.  │
#   └───────────────────────────────────────────────┘
#                          │
#                          ▼
#   STEP 1 — Coarse filter (cheap, plain SQL, no math yet)
#   ┌───────────────────────────────────────────────┐
#   │ fetch_candidates_with_vectors(shard_ids=[7])   │
#   │   SELECT entity_id, vector FROM entities       │
#   │   WHERE shard_id = ANY([7]) AND id != '18'     │
#   │                                                 │
#   │ Returns a GROUP, not "the similar ones" yet:   │
#   │   [("10", raw_bytes), ("16", raw_bytes),       │
#   │    ("121", raw_bytes)]                         │
#   └───────────────────────────────────────────────┘
#                          │
#                          ▼
#   STEP 2 — Decode raw bytes back into real numbers
#   ┌───────────────────────────────────────────────┐
#   │ np.frombuffer(raw, dtype="<f4")                │
#   │ raw_bytes  →  array([0.023, -0.118, ...])      │
#   │ (already done automatically inside Step 1)     │
#   └───────────────────────────────────────────────┘
#                          │
#                          ▼
#   STEP 3 — Get the ONE target vector to compare against
#   ┌───────────────────────────────────────────────┐
#   │ fetch_vector(dsn, "18")  →  entity 18's own    │
#   │ vector, decoded the same way as Step 2.        │
#   │ (this is the function still left to write)     │
#   └───────────────────────────────────────────────┘
#                          │
#                          ▼
#   STEP 4 — NOW compute real distance, one pair at a time
#            (the expensive step — but only across the SMALL
#            group Step 1 already narrowed things down to)
#   ┌───────────────────────────────────────────────┐
#   │ for each (id, vec) in candidates:              │
#   │     distance = cosine_distance(target, vec)    │
#   │                                                 │
#   │ ("10", 0.15)   ("16", 0.42)   ("121", 0.08)    │
#   └───────────────────────────────────────────────┘
#                          │
#                          ▼
#   STEP 5 — Sort ascending, keep the closest K
#   ┌───────────────────────────────────────────────┐
#   │ sorted by distance → [("121", 0.08),           │
#   │                        ("10", 0.15),           │
#   │                        ("16", 0.42)]           │
#   │                                                 │
#   │ THIS is what "similar vectors" means here —    │
#   │ a result you CALCULATE, never a property the   │
#   │ database already knew before you asked.        │
#   └───────────────────────────────────────────────┘
#
#   THE TWO-TIER DESIGN, in one line:
#   shard_id filter  = cheap & coarse   (a plain SQL WHERE clause)
#   cosine_distance  = expensive & precise (run ONLY on the small
#                      filtered group from Step 1 — never the whole table)
# ============================================================
```

## vector-hasher/app/heap.py

```python
import heapq

# vector-hasher/db.py/fetch_candidates_with_vectors gives the candidates with their vectors , 
# so that we can compute the distance between the new entity 
# and each candidate and build a heap of the closest candidates.

# INPUT — called from neighbor_index.py, in two different loops:
#   1) building a new entity's own list: heap starts as [], entry's distance
#      comes from lsh.cosine_distance(target_vector, candidate_vector).
#   2) the reverse eviction check on an existing candidate: heap is rebuilt
#      from db.fetch_neighbors_write(dsn, candidate_id)'s existing list,
#      entry is (same distance, new_entity_id).
#   k comes from the NEIGHBOR_INDEX_K config value either way.
# OUTPUT — the True/False return tells neighbor_index.py whether to bother
# calling db.save_neighbors (+ kafka_client.send_neighbor_update) for this
# entity at all. False means nothing changed — do nothing further.
def push_with_eviction(
    heap: list[tuple[float, str]], entry: tuple[float, str], k: int
) -> bool:
    distance, entity_id = entry
# //Extract the distance and entity ID from the incoming candidate

    # An entity already sitting in the heap (e.g. reloaded via a rebuild from
    # its persisted list) must not be treated as a new candidate — distance
    # is deterministic for a given pair, so a repeat push is never a genuine
    # update, only a duplicate waiting to happen.
    if any(existing_entity_id == entity_id for _, existing_entity_id in heap):
        return False

    if len(heap) < k:
        heapq.heappush(heap, (-distance, entity_id))
        return True

    farthest_distance = -heap[0][0]
    if distance < farthest_distance:
        heapq.heapreplace(heap, (-distance, entity_id))
        return True

    return False


# INPUT — called from neighbor_index.py, once per entity whose heap actually
# changed (i.e. at least one push_with_eviction call on it returned True).
# OUTPUT — the closest-first list gets handed to db.save_neighbors(...) /
# db.save_neighbors_read(...) (write-side, read-side) and to
# kafka_client.send_neighbor_update(...) — this is the final shape that
# ends up stored and published, nothing further transforms it.
def flatten_sorted(heap: list[tuple[float, str]]) -> list[tuple[float, str]]:
    return sorted((-neg_distance, entity_id) for neg_distance, entity_id in heap)


# pick 10 , build heap , checke if we can insert the nex entity ? 

# Here's the full sequence for new entity 18 arriving:

# Find candidates — LSH gives you entity 18's nearby bucket-mates, say [10, 16, 121].
# Build entity 18's own heap — compute distance(18, each candidate), build a fresh heap for 18's own brand-new list. This one's simple since 18 has no prior list.
# For each candidate separately, do the reverse check — this is the part worth being precise about:
# Fetch entity 10's existing list from Postgres → rebuild its own heap → check if 18 should evict 10's current farthest → if yes, write it back.
# Fetch entity 16's existing list from Postgres → rebuild its own separate heap → same check → maybe write back, maybe not.
# Fetch entity 121's list → same thing again.

# So it's not one heap serving the whole operation — it's N+1 heaps,
# one per entity touched (the new entity plus each candidate), 
# each built fresh, each checked and discarded independently.
```

## vector-hasher/app/kafka_client.py

```python
import json

from kafka import KafkaConsumer, KafkaProducer

from app import db
from app.cache import LRUCache

CONSUMER_GROUP_ID = "vector-hasher-neighbor-index-consumer"


def create_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def send_neighbor_update(
    producer: KafkaProducer,
    topic: str,
    entity_id: str,
    neighbors: list[dict[str, object]],
) -> None:
    # .get() blocks until the broker acknowledges (or raises on failure) —
    # keeps this genuinely synchronous instead of a fire-and-forget send.
    future = producer.send(topic, key=entity_id, value=neighbors)
    future.get(timeout=10)


def run_consumer(dsn: str, cache: LRUCache, topic: str, bootstrap_servers: str) -> None:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=CONSUMER_GROUP_ID,
        # earliest, not the kafka-python default of latest — a message
        # produced right before this thread finishes starting up must
        # still be seen, not silently skipped.
        auto_offset_reset="earliest",
        key_deserializer=lambda key: key.decode("utf-8"),
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    for message in consumer:
        entity_id = message.key
        neighbors = message.value
        db.save_neighbors_read(dsn, entity_id, neighbors)
        cache.invalidate(entity_id)

```

## vector-hasher/app/lsh.py

```python
from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class HashResult:
    hash_bits: list[int]
    shard_id: int


def generate_hyperplanes(
    num_hyperplanes: int, embedding_dim: int, seed: int
) -> np.ndarray:
    rng = np.random.default_rng(seed)
    return rng.standard_normal((num_hyperplanes, embedding_dim))


def compute_hash(
    vector: np.ndarray, hyperplanes: np.ndarray, num_shards: int
) -> HashResult:
    projections = hyperplanes @ vector
    hash_bits = [1 if value >= 0 else 0 for value in projections]

    hash_value = 0
    for bit in hash_bits:
        hash_value = (hash_value << 1) | bit

    return HashResult(hash_bits=hash_bits, shard_id=hash_value % num_shards)


def adjacent_shard_ids(shard_id: int, num_shards: int) -> list[int]:
    return sorted({(shard_id - 1) % num_shards, shard_id, (shard_id + 1) % num_shards})

# It only picks which shard numbers to look in — 
# e.g. it returns something like [3] or [2, 3, 4]. It does not fetch any actual articles.
# // this shardIDs ===> is passed to vector-hasher/db.py/fetch_candidates_with_vectors
def resolve_candidate_shard_ids(
    shard_id: int, same_bucket_count: int, num_shards: int, min_candidates: int
) -> list[int]:
    if same_bucket_count >= min_candidates:
        return [shard_id]
    return adjacent_shard_ids(shard_id, num_shards)

def fetch_shard_id(dsn: str, entity_id: str) -> int | None:
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT shard_id FROM entities WHERE entity_id = %s", (entity_id,)
            )
            row = cur.fetchone()
    return row[0] if row else None

# /////////////////////what did we save in the milestone 2 . because the same vectors , lie in the same shard . ?? isnt it ?? ans short
# that's the goal of LSH: similar vectors usually land in the same shard. 
# But not guaranteed — this project's own notes (PROGRESS.md) found a real '
# 'case where two very similar articles landed in different, non-adjacent shards, '
# 'just because of one unlucky bit. That's exactly why the "widen to adjacent shards"
# " if not enough candidates" fallback exists
def cosine_distance(vec1: np.ndarray, vec2: np.ndarray) -> float:
    dot_product = np.dot(vec1, vec2)
    norm_vec1 = np.linalg.norm(vec1)
    norm_vec2 = np.linalg.norm(vec2)
    return 1 - (dot_product / (norm_vec1 * norm_vec2))
# This is the number the heap sorts and evicts by. 
# Cosine distance isn't a separate concept sitting next to the heap — it's 
# literally the "distance" the heap logic is built around:






#             384 values
#         ┌──────────────────────┐
# HP1     │ • • • • • • • • • •  │
# HP2     │ • • • • • • • • • •  │
# HP3     │ • • • • • • • • • •  │
# ...     │                      │
# HP20    │ • • • • • • • • • •  │
#         └──────────────────────┘

# 20 Hyperplanes

# Hyperplane 1 → 384 numbers

# Hyperplane 2 → 384 numbers

# ...

# Hyperplane 20 → 384 numbers

# The dot product tells you which side of the hyperplane the vector lies on.

# Suppose one hyperplane is represented by a 384-dimensional vector h, and your embedding is another 384-dimensional vector v.

# You compute:

# projection = h · v

# Now interpret the sign:

# projection > 0
#         │
#         ▼
# One side of the hyperplane
# (bit = 1)


# One honest correction to my own plan text: I described these as
# "random unit vectors" — that's not quite what this does.
# standard_normal gives vectors of random magnitude, 
# not normalized to length 1. That's actually fine and deliberate: '
# 'for sign(dot(vector, r)), scaling r by any positive number never '
# 'changes the sign of the dot product — so whether r has length 1 or '
# 'length 7 makes zero difference to the resulting hash bit. Normalizing '
# 'would cost a sqrt + division per hyperplane for a result that's 
# mathematically identical either way, so skipping it isn't cutting '
# 'a corner — it's just not doing unnecessary work.
```

## vector-hasher/app/main.py

```python
import os
import threading
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import numpy as np
from fastapi import FastAPI, HTTPException
from kafka import KafkaProducer
from pydantic import BaseModel, ConfigDict, Field

from app import db, kafka_client, lsh, neighbor_index
from app.cache import LRUCache
# Reads the PostgreSQL connection string.
DATABASE_URL = os.environ["DATABASE_URL"]
LSH_SEED = int(os.environ.get("LSH_SEED", "42"))
LSH_NUM_HYPERPLANES = int(os.environ.get("LSH_NUM_HYPERPLANES", "20"))
NUM_SHARDS = int(os.environ.get("NUM_SHARDS", "8"))
EMBEDDING_DIM = int(os.environ.get("EMBEDDING_DIM", "384"))
MIN_CANDIDATES = int(os.environ.get("MIN_CANDIDATES", "5"))
KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
NEIGHBOR_INDEX_K = int(os.environ.get("NEIGHBOR_INDEX_K", "3"))
KAFKA_NEIGHBOR_TOPIC = os.environ.get("KAFKA_NEIGHBOR_TOPIC", "neighbor-updates")
NEIGHBOR_CACHE_SIZE = int(os.environ.get("NEIGHBOR_CACHE_SIZE", "32"))

_hyperplanes: np.ndarray | None = None
_kafka_producer: KafkaProducer | None = None
_neighbor_cache: LRUCache | None = None

# On startup: Generates the hyperplane matrix (fixed random vectors used for all hashing operations),
# creates the Kafka producer, creates the neighbor cache, and starts the Kafka
# consumer loop on a background thread (daemon=True — it never returns on its
# own, so it must not block process shutdown).
# On shutdown: Cleans up the global references.
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
#     When FastAPI starts, it generates the 20 random hyperplanes.
# These remain fixed throughout the lifetime of the application.
    global _hyperplanes, _kafka_producer, _neighbor_cache
    _hyperplanes = lsh.generate_hyperplanes(LSH_NUM_HYPERPLANES, EMBEDDING_DIM, LSH_SEED)
    _kafka_producer = kafka_client.create_producer(KAFKA_BOOTSTRAP_SERVERS)
    _neighbor_cache = LRUCache(NEIGHBOR_CACHE_SIZE)

    threading.Thread(
        target=kafka_client.run_consumer,
        args=(DATABASE_URL, _neighbor_cache, KAFKA_NEIGHBOR_TOPIC, KAFKA_BOOTSTRAP_SERVERS),
        daemon=True,
    ).start()

    yield

    _hyperplanes = None
    if _kafka_producer is not None:
        _kafka_producer.close()
    _kafka_producer = None
    _neighbor_cache = None


app = FastAPI(title="Vector Hasher", lifespan=lifespan)

# These classes describe what JSON the API accepts and returns.
class HashRequest(BaseModel):
    vector: list[float]


class HashResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    shard_id: int = Field(alias="shardId")
    hash_bits: list[int] = Field(alias="hashBits")


class AssignShardsResponse(BaseModel):
    total: int
    updated: int


class Candidate(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    entity_id: str = Field(alias="entityId")
    title: str


class CandidatesResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    entity_id: str = Field(alias="entityId")
    shard_id: int = Field(alias="shardId")
    candidates: list[Candidate]


class NeighborEntry(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    entity_id: str = Field(alias="entityId")
    distance: float


class IndexResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    entity_id: str = Field(alias="entityId")
    changed: list[str]


class IndexAllResponse(BaseModel):
    total: int
    changed: int


class NeighborsResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    entity_id: str = Field(alias="entityId")
    neighbors: list[NeighborEntry]


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}

# Takes a vector (list of floats)
# Validates it has correct dimensionality
# Computes its LSH hash (bit signature + shard ID)
# Returns which shard the vector belongs to and its hash bits

# Use case: Client-side hashing or immediate shard lookup.
# /hash — Hash one vector

# Imagine a client sends one embedding.
@app.post("/hash", response_model=HashResponse)
def hash_vector(request: HashRequest) -> HashResponse:
    if _hyperplanes is None:
        raise RuntimeError("Hyperplanes are not initialized")
    if len(request.vector) != EMBEDDING_DIM:
        raise HTTPException(
            status_code=400,
            detail=f"Expected a {EMBEDDING_DIM}-dim vector, got {len(request.vector)}",
        )
# Compute the hash and shard ID for a single embedding vector.
    vector = np.array(request.vector, dtype="<f4")
    result = lsh.compute_hash(vector, _hyperplanes, NUM_SHARDS)
    # which FastAPI converts into JSON.
    return HashResponse(shard_id=result.shard_id, hash_bits=result.hash_bits)

# Fetches all vectors from the database
# Computes their shard IDs using LSH
# Writes shard assignments back to the database
# Returns total processed count

# /assign-shards — Hash every vector in the database

# Instead of one vector, it processes all stored vectors.
@app.post("/assign-shards", response_model=AssignShardsResponse)
def assign_shards() -> AssignShardsResponse:
    if _hyperplanes is None:
        raise RuntimeError("Hyperplanes are not initialized")

    rows = db.fetch_all_vectors(DATABASE_URL)
    assignments = [
        (entity_id, lsh.compute_hash(vector, _hyperplanes, NUM_SHARDS).shard_id)
        for entity_id, vector in rows
    ]
    db.update_shard_ids(DATABASE_URL, assignments)
    return AssignShardsResponse(total=len(rows), updated=len(assignments))

# It first checks how many articles are in the same shard as the entity.
# If that's enough (>= MIN_CANDIDATES), it only searches that one shard.
# If not enough, it widens the search to that shard's neighbors too (using adjacent_shard_ids, already in lsh.py).
# Find which shard the given article belongs to.
@app.get("/candidates/{entity_id}", response_model=CandidatesResponse)
def candidates(entity_id: str) -> CandidatesResponse:
    shard_id = db.fetch_shard_id(DATABASE_URL, entity_id)
    if shard_id is None:
        raise HTTPException(status_code=404, detail=f"Unknown entityId: {entity_id}")
# Retrieve candidate articles from the same shard (excluding the current article).
    same_bucket = db.fetch_candidates(DATABASE_URL, [shard_id], entity_id)
    shard_ids = lsh.resolve_candidate_shard_ids(
        shard_id, len(same_bucket), NUM_SHARDS, MIN_CANDIDATES
    )
    results = (
        same_bucket
        if shard_ids == [shard_id]
        else db.fetch_candidates(DATABASE_URL, shard_ids, entity_id)
    )

    return CandidatesResponse(
        entity_id=entity_id,
        shard_id=shard_id,
        candidates=[Candidate(entity_id=c["entityId"], title=c["title"]) for c in results],
    )


# Runs the full index_entity pipeline for one entity: build its own list,
# reverse-check every candidate, save + publish whatever changed.
@app.post("/neighbors/{entity_id}/index", response_model=IndexResponse)
def index_neighbor(entity_id: str) -> IndexResponse:
    if _kafka_producer is None:
        raise RuntimeError("Kafka producer is not initialized")
    try:
        changed = neighbor_index.index_entity(
            DATABASE_URL,
            _kafka_producer,
            entity_id,
            NEIGHBOR_INDEX_K,
            NUM_SHARDS,
            MIN_CANDIDATES,
            KAFKA_NEIGHBOR_TOPIC,
        )
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Unknown entityId: {entity_id}")

    return IndexResponse(entity_id=entity_id, changed=changed)


# Bootstraps the whole current dataset — same role /assign-shards played in
# Milestone 2. Runs index_entity once per entity currently in `entities`.
@app.post("/neighbors/index-all", response_model=IndexAllResponse)
def index_all_neighbors() -> IndexAllResponse:
    if _kafka_producer is None:
        raise RuntimeError("Kafka producer is not initialized")

    entity_ids = [entity_id for entity_id, _ in db.fetch_all_vectors(DATABASE_URL)]
    total_changed = 0
    for entity_id in entity_ids:
        changed = neighbor_index.index_entity(
            DATABASE_URL,
            _kafka_producer,
            entity_id,
            NEIGHBOR_INDEX_K,
            NUM_SHARDS,
            MIN_CANDIDATES,
            KAFKA_NEIGHBOR_TOPIC,
        )
        total_changed += len(changed)

    return IndexAllResponse(total=len(entity_ids), changed=total_changed)


# Read-side lookup: cache first, neighbor_index_read on a miss (and re-caches
# the result so the next read for the same entity is a hit).
@app.get("/neighbors/{entity_id}", response_model=NeighborsResponse)
def get_neighbors(entity_id: str) -> NeighborsResponse:
    if _neighbor_cache is None:
        raise RuntimeError("Neighbor cache is not initialized")

    neighbors = _neighbor_cache.get(entity_id)
    if neighbors is None:
        neighbors = db.fetch_neighbors_read(DATABASE_URL, entity_id)
        if neighbors is None:
            raise HTTPException(status_code=404, detail=f"Unknown entityId: {entity_id}")
        _neighbor_cache.set(entity_id, neighbors)

    return NeighborsResponse(
        entity_id=entity_id,
        neighbors=[
            NeighborEntry(entity_id=n["entityId"], distance=n["distance"]) for n in neighbors
        ],
    )

```

## vector-hasher/app/neighbor_index.py

```python
from kafka import KafkaProducer

from app import db, heap, kafka_client, lsh

# ============================================================
# index_entity(entity_id) — FLOW MAP, both halves
# ============================================================
#
# INPUT: entity_id (the entity being indexed — brand new, or re-indexed)
#
#   SETUP (runs once, before either half)
#   ┌─────────────────────────────────────────────────────┐
#   │ db.fetch_shard_id(entity_id)      → shard_id         │
#   │ db.fetch_vector(entity_id)        → vector           │
#   │ db.fetch_candidates_with_vectors  → same_bucket       │
#   │ lsh.resolve_candidate_shard_ids   → shard_ids to use  │
#   │   (re-fetch with fetch_candidates_with_vectors if     │
#   │    the widened list differs from [shard_id])          │
#   │ → candidates: [(candidate_id, candidate_vector), ...] │
#   └─────────────────────────────────────────────────────┘
#                             │
#            ┌────────────────┴────────────────┐
#            ▼              
#                     ▼
#   HALF 1 — entity's OWN list          HALF 2 — reverse check
#   (always runs, always saves)         (per candidate, may do nothing)
#   ┌─────────────────────────┐         ┌──────────────────────────────┐
#   │ for each candidate:      │         │ for each candidate:           │
#   │  lsh.cosine_distance     │         │  db.fetch_neighbors_write     │
#   │   (vector, cand_vector)  │         │   (candidate_id)              │
#   │   → distance             │         │   → existing (candidate's     │
#   │  heap.push_with_eviction │         │      CURRENT saved list)      │
#   │   (own_heap,             │         │  _rebuild_heap(existing, k)   │
#   │    (distance, cand_id))  │         │   → candidate_heap            │
#   │                          │         │  heap.push_with_eviction      │
#   │ heap.flatten_sorted      │         │   (candidate_heap,            │
#   │  (own_heap)              │         │    (distance, entity_id))     │
#   │  → own_neighbors         │         │   → True/False                │
#   │                          │         │                                │
#   │ db.save_neighbors(...)   │         │  False → STOP, nothing saved  │
#   │ kafka_client.            │         │  for this candidate            │
#   │  send_neighbor_update    │         │                                │
#   │  (..., entity_id, ...)   │         │  True → db.fetch_shard_id      │
#   │                          │         │   (candidate_id)               │
#   │ ALWAYS runs — this list  │         │   heap.flatten_sorted(...)     │
#   │ is brand new             │         │   db.save_neighbors(...)       │
#   │                          │         │   kafka_client.                │
#   │                          │         │    send_neighbor_update        │
#   │                          │         │    (..., candidate_id, ...)    │
#   └───────────────────────── ┘          └──────────────────────────────┘
# 
#   Note: distance is computed ONCE in Half 1 and reused in Half 2 for the
#   same candidate — cosine distance is symmetric, so distance(A, B) ==
#   distance(B, A). No need to call cosine_distance a second time.
#
# OUTPUT: changed_entity_ids — entity_id (always) + any candidate_ids whose
# list actually got evicted-into (only the ones where push_with_eviction
# returned True in Half 2).
# ============================================================


def index_entity(
    dsn: str,
    producer: KafkaProducer,
    entity_id: str,
    k: int,
    num_shards: int,
    min_candidates: int,
    topic: str,
) -> list[str]:
    shard_id = db.fetch_shard_id(dsn, entity_id)
    vector = db.fetch_vector(dsn, entity_id)
    if shard_id is None or vector is None:
        raise ValueError(f"Unknown entityId: {entity_id}")

    # Same-bucket-first, widen-if-too-few — same fallback /candidates uses.
    same_bucket = db.fetch_candidates_with_vectors(dsn, [shard_id], entity_id)
    shard_ids = lsh.resolve_candidate_shard_ids(
        shard_id, len(same_bucket), num_shards, min_candidates
    )
    candidates = (
        same_bucket
        if shard_ids == [shard_id]
        else db.fetch_candidates_with_vectors(dsn, shard_ids, entity_id)
    )

    changed_entity_ids: list[str] = []

    # --- Half 1: build the new entity's own list. ---
    # cosine_distance's output feeds straight into push_with_eviction; the
    # same distance value gets reused below in the reverse check (distance
    # is symmetric, so no need to compute it twice per candidate).
    own_heap: list[tuple[float, str]] = []
    distances: dict[str, float] = {}

    for candidate_id, candidate_vector in candidates:
        distance = lsh.cosine_distance(vector, candidate_vector)
        distances[candidate_id] = distance
        heap.push_with_eviction(own_heap, (distance, candidate_id), k)

    # Always save + publish — this list is brand new, so it always changed.
    own_neighbors = heap.flatten_sorted(own_heap)
    db.save_neighbors(dsn, entity_id, shard_id, own_neighbors)
    kafka_client.send_neighbor_update(producer, topic, entity_id, _to_json(own_neighbors))
    changed_entity_ids.append(entity_id)

# without it, only the brand-new entity would ever get an updated list
# — existing entities would never find out a closer neighbor just showed up.

    # --- Half 2: reverse check — does the new entity now belong in each
    # candidate's own existing list? ---
    
    for candidate_id, _ in candidates:
        existing = db.fetch_neighbors_write(dsn, candidate_id)
        candidate_heap = _rebuild_heap(existing, k)

        changed = heap.push_with_eviction(
            candidate_heap, (distances[candidate_id], entity_id), k
        )
        if not changed:
            continue

        candidate_shard_id = db.fetch_shard_id(dsn, candidate_id)
        candidate_neighbors = heap.flatten_sorted(candidate_heap)
        db.save_neighbors(dsn, candidate_id, candidate_shard_id, candidate_neighbors)
        kafka_client.send_neighbor_update(
            producer, topic, candidate_id, _to_json(candidate_neighbors)
        )
        changed_entity_ids.append(candidate_id)

    return changed_entity_ids


# Reloads an already-persisted (already-within-k) list back into heap form.
# No eviction actually happens here — push_with_eviction is just reused as
# the insertion primitive, since heap.py doesn't expose a separate "load
# without eviction" function.
def _rebuild_heap(existing: list[dict[str, object]] | None, k: int) -> list[tuple[float, str]]:
    rebuilt: list[tuple[float, str]] = []
    if existing is None:
        return rebuilt
    for item in existing:
        heap.push_with_eviction(rebuilt, (item["distance"], item["entityId"]), k)
    return rebuilt


# flatten_sorted's tuples -> the {"entityId", "distance"} shape
# kafka_client.send_neighbor_update (and, on the consumer side,
# db.save_neighbors_read) expect on the wire.
def _to_json(neighbors: list[tuple[float, str]]) -> list[dict[str, object]]:
    return [{"entityId": entity_id, "distance": distance} for distance, entity_id in neighbors]

```

## vector-hasher/requirements.txt

```txt
fastapi==0.115.0
uvicorn[standard]==0.30.6
numpy==1.26.4
psycopg[binary]==3.2.3
kafka-python==2.0.2
pytest==8.3.3

```

## vector-hasher/tests/README.md

````markdown
# vector-hasher tests

## How to run

```
python3 -m venv .venv
source .venv/bin/activate
pip install numpy pytest
python -m pytest -v
```

No database, no Docker, no other services required — every test here exercises
`app/lsh.py` only (pure functions, zero I/O).

## Last run

```
============================= test session starts ==============================
platform linux -- Python 3.12.3, pytest-9.1.1, pluggy-1.6.0
collecting ... collected 5 items

tests/test_lsh.py::test_same_vector_always_hashes_to_the_same_shard PASSED [ 20%]
tests/test_lsh.py::test_same_seed_produces_the_same_hyperplanes_across_runs PASSED [ 40%]
tests/test_lsh.py::test_nearly_identical_vectors_land_in_the_same_bucket PASSED [ 60%]
tests/test_lsh.py::test_opposite_vectors_produce_fully_complementary_hash_bits PASSED [ 80%]
tests/test_lsh.py::test_adjacent_shard_ids_wraps_around_correctly PASSED [100%]

============================== 5 passed in 0.13s ===============================
```

## What each test checks, and why

### `test_same_vector_always_hashes_to_the_same_shard`
Hashes the same vector twice against the same hyperplanes and asserts both the
`shard_id` and the raw `hash_bits` are identical both times. Basic determinism
check — guards against any accidental randomness inside `compute_hash` itself.

### `test_same_seed_produces_the_same_hyperplanes_across_runs`
Calls `generate_hyperplanes` twice, independently, with the same seed, and
asserts the two resulting matrices are exactly equal (`np.array_equal`).

This is the single most important property in the whole service: if the
hyperplanes were re-randomized every time the process started, every
previously-assigned `shard_id` in Postgres would become meaningless the moment
the container restarted, since the same vector would hash differently before
and after. This test proves the fixed-seed design actually delivers that
stability, rather than just asserting it in a comment.

### `test_nearly_identical_vectors_land_in_the_same_bucket`
Builds a base vector, then a second vector nudged by noise roughly six orders
of magnitude smaller than the base vector's own values, and asserts both land
in the same `shard_id`. This is the core LSH property Milestone 2 exists to
demonstrate: similar input -> same bucket. Because the random seeds used to
build the test vectors are fixed, this test is fully deterministic — it is not
a "usually passes" test despite what it sounds like conceptually.

### `test_opposite_vectors_produce_fully_complementary_hash_bits`
Hashes a vector and its exact negation, and asserts the two `hash_bits` lists
differ. A vector and its negation are guaranteed to flip the sign of every
single hyperplane projection, so `hash_bits` is certain to differ in every
position — this isn't a probabilistic check.

Deliberately asserts on `hash_bits`, not `shard_id`. `shard_id = hash_value %
8` only depends on 3 of the 20 hash bits, so it's a lossy compression: two
fully-opposite hash patterns (all-0s vs all-1s) can still reduce to the *same*
`shard_id`, since 2^20 is itself divisible by 8. Asserting `shard_id` differs
here would be a false assumption; asserting `hash_bits` differ is the property
that's actually always true.

### `test_adjacent_shard_ids_wraps_around_correctly`
Three hand-verified cases for the "adjacent bucket" fallback used by
`/candidates`: `shard_id=0`'s neighbors are `7` and `1` (wraps around the low
end), `shard_id=7`'s neighbors are `6` and `0` (wraps around the high end),
and `shard_id=3` (an ordinary middle case) gives `2` and `4`. Directly tests
the modular wraparound arithmetic, the kind of boundary logic that's easy to
get subtly wrong without an explicit test.

````

## vector-hasher/tests/__init__.py

```python

```

## vector-hasher/tests/test_heap.py

```python
from app.heap import flatten_sorted, push_with_eviction

K = 3


def test_pushing_fewer_than_k_entries_always_inserts() -> None:
    heap: list[tuple[float, str]] = []

    changed = push_with_eviction(heap, (0.5, "A"), K)

    assert changed is True
    assert len(heap) == 1


def test_pushing_a_closer_entry_once_full_evicts_only_the_farthest() -> None:
    heap: list[tuple[float, str]] = []
    push_with_eviction(heap, (0.1, "A"), K)
    push_with_eviction(heap, (0.5, "B"), K)
    push_with_eviction(heap, (0.9, "C"), K)

    changed = push_with_eviction(heap, (0.3, "D"), K)

    assert changed is True
    assert flatten_sorted(heap) == [(0.1, "A"), (0.3, "D"), (0.5, "B")]


def test_pushing_a_farther_entry_once_full_is_a_no_op() -> None:
    heap: list[tuple[float, str]] = []
    push_with_eviction(heap, (0.1, "A"), K)
    push_with_eviction(heap, (0.5, "B"), K)
    push_with_eviction(heap, (0.9, "C"), K)

    changed = push_with_eviction(heap, (1.5, "Z"), K)

    assert changed is False
    assert flatten_sorted(heap) == [(0.1, "A"), (0.5, "B"), (0.9, "C")]


def test_flatten_sorted_returns_closest_first_order() -> None:
    heap: list[tuple[float, str]] = []
    push_with_eviction(heap, (0.9, "C"), K)
    push_with_eviction(heap, (0.1, "A"), K)
    push_with_eviction(heap, (0.5, "B"), K)

    assert flatten_sorted(heap) == [(0.1, "A"), (0.5, "B"), (0.9, "C")]


def test_pushing_an_entity_already_in_the_heap_is_a_no_op_not_a_duplicate() -> None:
    heap: list[tuple[float, str]] = []
    push_with_eviction(heap, (0.1, "A"), K)
    push_with_eviction(heap, (0.5, "B"), K)

    changed = push_with_eviction(heap, (0.5, "B"), K)

    assert changed is False
    assert flatten_sorted(heap) == [(0.1, "A"), (0.5, "B")]


def test_pushing_an_entity_already_in_a_full_heap_does_not_evict_anything() -> None:
    heap: list[tuple[float, str]] = []
    push_with_eviction(heap, (0.1, "A"), K)
    push_with_eviction(heap, (0.5, "B"), K)
    push_with_eviction(heap, (0.9, "C"), K)

    changed = push_with_eviction(heap, (0.9, "C"), K)

    assert changed is False
    assert flatten_sorted(heap) == [(0.1, "A"), (0.5, "B"), (0.9, "C")]

```

## vector-hasher/tests/test_lsh.py

```python
import numpy as np

from app.lsh import adjacent_shard_ids, compute_hash, generate_hyperplanes

SEED = 42
NUM_HYPERPLANES = 20
EMBEDDING_DIM = 384
NUM_SHARDS = 8


def test_same_vector_always_hashes_to_the_same_shard() -> None:
    hyperplanes = generate_hyperplanes(NUM_HYPERPLANES, EMBEDDING_DIM, SEED)
    vector = np.random.default_rng(1).standard_normal(EMBEDDING_DIM)

    first = compute_hash(vector, hyperplanes, NUM_SHARDS)
    second = compute_hash(vector, hyperplanes, NUM_SHARDS)

    assert first.shard_id == second.shard_id
    assert first.hash_bits == second.hash_bits


def test_same_seed_produces_the_same_hyperplanes_across_runs() -> None:
    first_run = generate_hyperplanes(NUM_HYPERPLANES, EMBEDDING_DIM, SEED)
    second_run = generate_hyperplanes(NUM_HYPERPLANES, EMBEDDING_DIM, SEED)

    assert np.array_equal(first_run, second_run)


def test_nearly_identical_vectors_land_in_the_same_bucket() -> None:
    hyperplanes = generate_hyperplanes(NUM_HYPERPLANES, EMBEDDING_DIM, SEED)
    rng = np.random.default_rng(2)
    base_vector = rng.standard_normal(EMBEDDING_DIM)
    nudged_vector = base_vector + rng.standard_normal(EMBEDDING_DIM) * 1e-6

    base_result = compute_hash(base_vector, hyperplanes, NUM_SHARDS)
    nudged_result = compute_hash(nudged_vector, hyperplanes, NUM_SHARDS)

    assert base_result.shard_id == nudged_result.shard_id


def test_opposite_vectors_produce_fully_complementary_hash_bits() -> None:
    hyperplanes = generate_hyperplanes(NUM_HYPERPLANES, EMBEDDING_DIM, SEED)
    vector = np.random.default_rng(3).standard_normal(EMBEDDING_DIM)
    opposite_vector = -vector

    result = compute_hash(vector, hyperplanes, NUM_SHARDS)
    opposite_result = compute_hash(opposite_vector, hyperplanes, NUM_SHARDS)

    assert result.hash_bits != opposite_result.hash_bits


def test_adjacent_shard_ids_wraps_around_correctly() -> None:
    assert adjacent_shard_ids(0, NUM_SHARDS) == [0, 1, 7]
    assert adjacent_shard_ids(7, NUM_SHARDS) == [0, 6, 7]
    assert adjacent_shard_ids(3, NUM_SHARDS) == [2, 3, 4]

```

## CLAUDE.md

```markdown
# CLAUDE.md — Stable Project Context

This file is read automatically at the start of every session. Keep it lean — full design rationale, data flow, and milestone details live in `PROJECT_SPEC.md`; read that separately when starting a milestone.

## What this project is
A two-stage (retrieval → ranking) news recommendation engine, built on the MIND-small dataset, demonstrating vector-hash sharding, a dual-sharded neighbor index, Kafka-based propagation, Redis-backed serving state, and Bloom-filter-based seen-item filtering.

## Tech stack
- Java 21 / Spring Boot — Entity Upload Service, Entity Interaction Service, Recommendation Service
- Python 3.11 / FastAPI — Embedding Creator, vector hashing / LSH, neighbor computation
- PostgreSQL — Entity DB, Entity History DB, Neighbor Index (both write and read copies)
- Redis — last-N entity cache + Bloom filter, sharded via `hash(userId) % NUM_SHARDS`
- Kafka (KRaft mode, no Zookeeper) — interaction events (key = userId), neighbor index resync (key = entityId). Inside Docker, services connect via `kafka:29092` (INTERNAL listener), not `localhost:9092`.
- Docker Compose — local orchestration for all of the above

## Folder conventions
- One top-level directory per service (see `PROJECT_SPEC.md` §9 for the full tree).
- Java services: standard Spring Boot Maven layout (`src/main/java`, `src/test/java`).
- Python services: FastAPI app in `app/`, tests in `tests/`, `requirements.txt` per service — no shared monorepo Python venv.

## Coding style
- Java: standard Spring Boot conventions, constructor injection only (no field `@Autowired`).
- Python: type hints required on all function signatures, `black` formatting.
- No inline secrets or API keys anywhere — use `.env` files, already gitignored.

## Build / test commands
- Java services: `mvn clean install` then `mvn test` inside each service directory.
- Python services: `pytest` inside each service directory.
- Full local stack: `docker-compose up` from repo root.

## Don't touch
- `scripts/mind-loader/` is a one-off ETL script, not a service — don't refactor it into the service architecture.
- Shard counts (`NUM_SHARDS`) are config-driven constants — don't hardcode shard math inline anywhere.

## Working agreement
- Always enter Plan Mode before editing code for any non-trivial change.
- Always run and show real test/build output before declaring a task done — never accept "should work" as verification.
- Log any simplification or trade-off made during implementation in `PROJECT_SPEC.md` §10 (Defensibility Tracker).

```

## docker-compose.yml

```yaml
version: '3.8'

services:
  kafka:
    image: apache/kafka:3.9.0
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093,INTERNAL://:29092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092,INTERNAL://kafka:29092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_NUM_PARTITIONS: 6
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      KAFKA_LOG_DIRS: /var/lib/kafka/data
    volumes:
      - kafka-data:/var/lib/kafka/data

  redis:
    image: redis:7.2-alpine
    container_name: redis
    ports:
      - "6379:6379"
    volumes:
      - redis-data:/data

  postgres:
    image: postgres:15-alpine
    container_name: postgres
    ports:
      - "5432:5432"
    environment:
      POSTGRES_DB: recengine
      POSTGRES_USER: recuser
      POSTGRES_PASSWORD: recpass
    volumes:
      - postgres-data:/var/lib/postgresql/data
      # Postgres, when it starts against a completely empty data volume, auto-runs any 
      #.sql files it finds in /docker-entrypoint-initdb.d/ — and docker-compose.yml already mounts ./database/init to that exact path
      - ./database/init:/docker-entrypoint-initdb.d

  entity-upload-service:
    build: ./entity-upload-service
    container_name: entity-upload-service
    ports:
      - "8081:8080"
    environment:
      EMBEDDING_CREATOR_URL: http://embedding-creator:8000
      SPRING_DATASOURCE_URL: jdbc:postgresql://postgres:5432/recengine
      SPRING_DATASOURCE_USERNAME: recuser
      SPRING_DATASOURCE_PASSWORD: recpass
      MIND_NEWS_TSV_PATH: /data/mind-small/train/news.tsv
    volumes:
      - ./data/mind-small:/data/mind-small:ro
    depends_on:
      - postgres
      - embedding-creator

  entity-interaction-service:
    build: ./entity-interaction-service
    container_name: entity-interaction-service
    ports:
      - "8082:8080"
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:29092
      KAFKA_INTERACTION_TOPIC: interaction-events
      SPRING_DATASOURCE_URL: jdbc:postgresql://postgres:5432/recengine
      SPRING_DATASOURCE_USERNAME: recuser
      SPRING_DATASOURCE_PASSWORD: recpass
      REDIS_HOST: redis
      REDIS_PORT: "6379"
      MIND_BEHAVIORS_TRAIN_TSV_PATH: /data/mind-small/train/behaviors.tsv
      MIND_BEHAVIORS_DEV_TSV_PATH: /data/mind-small/dev/behaviors.tsv
      LAST_N_SIZE: "5"
      REDIS_SHARD_COUNT: "8"
    volumes:
      - ./data/mind-small:/data/mind-small:ro
    depends_on:
      - kafka
      - postgres
      - redis

  recommendation-service:
    build: ./recommendation-service
    container_name: recommendation-service
    ports:
      - "8083:8080"
    environment:
      SPRING_DATASOURCE_URL: jdbc:postgresql://postgres:5432/recengine
      SPRING_DATASOURCE_USERNAME: recuser
      SPRING_DATASOURCE_PASSWORD: recpass
      REDIS_HOST: redis
      REDIS_PORT: 6379
      VECTOR_HASHER_URL: http://vector-hasher:8000
      LAST_N_SIZE: "5"
    depends_on:
      - redis
      - postgres
      - vector-hasher

  embedding-creator:
    build: ./embedding-creator
    container_name: embedding-creator
    ports:
      - "8000:8000"

  vector-hasher:
    build: ./vector-hasher
    container_name: vector-hasher
    ports:
      - "8001:8000"
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:29092
      DATABASE_URL: postgresql://recuser:recpass@postgres:5432/recengine
      LSH_SEED: "42"
      LSH_NUM_HYPERPLANES: "20"
      NUM_SHARDS: "8"
      EMBEDDING_DIM: "384"
      NEIGHBOR_INDEX_K: "3"
      KAFKA_NEIGHBOR_TOPIC: "neighbor-updates"
      NEIGHBOR_CACHE_SIZE: "32"
    depends_on:
      - postgres
      - kafka
      - redis

volumes:
  kafka-data:
  redis-data:
  postgres-data:

```

## MILESTONE_3_PLAN.md

````markdown
# Milestone 3 — Neighbor Index Write-Side + Kafka Resync: Plan

## Context

Milestones 1 and 2 are done and verified: real MIND-small articles are embedded, stored, and now carry real `shard_id` LSH bucket assignments. `vector-hasher`'s `/candidates/{entityId}` can already narrow "who's near this entity" down to a small candidate set — but it does no ranking and computes no real distance; per `PROJECT_SPEC.md`'s own architecture diagram, that's deliberately deferred to this milestone: *"Real cosine similarity is then computed only within that small candidate set."*

Milestone 3's job, per `PROJECT_SPEC.md` §7: maintain a real per-entity top-K nearest-neighbor list (max-heap, so eviction is O(log K) and "who's currently farthest" is O(1) to check), propagate every change through Kafka, and keep an entity-hash-sharded **read-side** copy plus a small cache in sync. The stated verify criterion is explicit and bidirectional: *"insert a new article similar to an existing one; confirm the **existing** entity's neighbor list updates (eviction happens correctly), and the read-side copy reflects it after Kafka consumption."* That means indexing a new entity must also reach back and update already-indexed entities' own heaps when the new entity is now one of their top-K — not just build the new entity's own list.

**Resolved before this plan:** this lives inside the existing `vector-hasher` service, not a new one. Two independent signals in the existing docs point the same direction: `docker-compose.yml`'s service list has no second stub for a "neighbor-index" service (only `vector-hasher`'s), and `PROJECT_SPEC.md` §9 explicitly labels the directory `# Python — LSH + neighbor computation` — bundling both jobs in the spec's own words. The architecture diagram's separate "Neighbor Index (write)" box is read as a logical separation of responsibility for explanation purposes, not a literal claim of a separate deployable service — reinforced by the fact the write-side's own shard key (`simhash_bucket % N`) *is* `vector-hasher`'s own `shard_id` output, not an independently-computed value.

---

## 1. File tree (additions to the existing `vector-hasher/` service)

```
vector-hasher/
├── app/
│   ├── lsh.py            # existing (M2) — ADD: cosine_distance(v1, v2) -> float
│   ├── heap.py            # NEW — pure max-heap logic, no I/O (mirrors lsh.py's separation)
│   ├── cache.py            # NEW — small manually-invalidatable LRU cache, no I/O
│   ├── db.py              # existing (M2) — ADD: read/write functions for the two new tables
│   ├── kafka_client.py     # NEW — thin producer.send() + consumer-loop wrappers
│   ├── neighbor_index.py   # NEW — orchestrator: candidates -> distances -> heap updates -> persist -> produce
│   └── main.py             # existing (M2) — ADD: 3 new endpoints + background Kafka consumer in lifespan
├── tests/
│   ├── test_lsh.py         # existing (M2)
│   └── test_heap.py        # NEW — pure heap eviction/ordering tests, no I/O
database/init/
└── 002_neighbor_index_schema.sql   # NEW — two tables, auto-run alongside 001_schema.sql
```

Same separation-of-concerns habit as every prior file: pure logic (`heap.py`, `cache.py`, `lsh.py`) stays testable without I/O; `db.py`/`kafka_client.py` isolate the two external systems; `neighbor_index.py` is the only file that touches both, mirroring how `IngestionService.java` was the only class touching both `EmbeddingCreatorClient` and `EntityRepository`.

---

## 2. New Postgres tables (`002_neighbor_index_schema.sql`)

```sql
CREATE TABLE neighbor_index_write (
    entity_id   VARCHAR(32)  PRIMARY KEY,
    shard_id    INTEGER      NOT NULL,
    neighbors   JSONB        NOT NULL,   -- [{"entityId": "...", "distance": 0.42}, ...] sorted ascending
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE neighbor_index_read (
    entity_id   VARCHAR(32)  PRIMARY KEY,
    neighbors   JSONB        NOT NULL,
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
```

Two tables, not one, deliberately — mirrors the spec's own write/read split (`shard: vector-hash` vs `shard: entity-hash`). In this single-Postgres-instance demo they're both just columns/tables, not physically separate stores (same logical-sharding scope decision as everywhere else in this project) — but keeping them as two tables still correctly models "the write-side heap-maintenance path never gets read by the serving path directly; only Kafka-consumed data does," which is the actual architectural point being demonstrated.

---

## 3. The algorithm (`heap.py` + `neighbor_index.py`)

`heap.py` — pure, no I/O:
- `push_with_eviction(heap, entry, k)` — if the heap has fewer than `k` entries, insert unconditionally. Otherwise peek the root (farthest); if the new entry is closer, pop-and-push (evict); otherwise leave the heap untouched. Returns whether a change happened, since that's exactly what decides whether to persist/produce to Kafka.
- `flatten_sorted(heap)` — turns the heap into a plain list sorted ascending by distance (closest first), matching `PROJECT_SPEC.md`'s `neighbors: [{id, distance}, ...]` shape and the "flat sorted array" read-side structure from §6.
- Python's built-in `heapq` is a **min-heap**; to get max-heap-by-distance behavior (farthest at the root, so eviction is a cheap peek), distances get negated on the way in and back out — a well-known, standard trick, not a deviation from the design.

`neighbor_index.py` — orchestrates indexing one entity, and this is the piece that satisfies the bidirectional verify requirement:
1. Look up the entity's `shard_id` (already computed by M2) and its vector.
2. Fetch its candidate set (reuses `db.fetch_candidates`, same same-bucket-then-adjacent-fallback logic from M2 — no new candidate-retrieval code needed).
3. Compute real `cosine_distance` (new `lsh.py` function) between the entity and every candidate.
4. Build the entity's **own** heap from those distances (bounded to `NEIGHBOR_INDEX_K`).
5. **For every candidate**, also check the *reverse* direction: fetch that candidate's existing heap from `neighbor_index_write`, rebuild it in memory, and call `push_with_eviction` with the new entity. If it changed, persist that candidate's updated heap too and produce a Kafka message for it.
6. Every heap that changed (the new entity's own, plus any existing entity whose heap was mutated) gets flattened, written to `neighbor_index_write`, and produced to the `neighbor-updates` Kafka topic (key = entityId, matching `CLAUDE.md`'s stated Kafka key convention).

**One deliberately small, config-driven parameter worth flagging now:** `NEIGHBOR_INDEX_K` defaults to **3**, not a large number. With only 20 real rows spread across 7 populated shards (2-5 entities each), a large K would mean every entity's candidate set never actually fills up — eviction would never be exercised by real data, and the milestone's own stated verify criterion ("confirm eviction happens correctly") would be untestable at this data scale. K=3 is small enough that indexing even our current 20-row dataset will genuinely exercise eviction, and it's a plain env var, trivial to raise later once more data exists.

---

## 4. Kafka (`kafka_client.py`) and the read-side consumer

- **Producer**: synchronous `kafka-python` `KafkaProducer`, one `send()` per changed heap, keyed by `entityId` (guarantees per-entity ordering, per `CLAUDE.md`).
- **Consumer**: a background thread, started from `main.py`'s existing `lifespan` handler (alongside hyperplane generation), running a blocking consume loop for the service's lifetime. On every message: overwrite the matching row in `neighbor_index_read` wholesale (not a diff — matches `PROJECT_SPEC.md` §5.3's explicit "full list, not a diff... idempotent, safe to replay"), and invalidate that `entityId` in the LRU cache.
- Runs as a thread, not a second process/service — consistent with the "no new service" resolution; FastAPI's own event loop keeps serving HTTP requests independently of it.

## 5. The cache (`cache.py`)

A small manually-managed LRU (`OrderedDict`-based, not `functools.lru_cache`) sitting in front of `GET /neighbors/{entityId}` reads from `neighbor_index_read`. Explicitly **not** `functools.lru_cache`: that decorator has no way to invalidate a single key, and an invalidation hook is not optional here — without it, the cache would keep serving stale neighbor lists forever after a Kafka-driven update, which would demonstrate the *wrong* thing rather than the caching concept. The consumer thread invalidates on every write it makes.

**Simplification worth naming honestly, matching the project's habit:** this cache is in-process only, not Redis-backed. At real scale, with multiple horizontally-scaled instances, an in-process cache wouldn't be shared and you'd want Redis; here, `vector-hasher` isn't wired to Redis at all yet, and adding that wiring just for this one milestone's cache isn't justified by what this milestone needs to demonstrate.

---

## 6. New endpoints (`main.py`)

- **`POST /neighbors/{entity_id}/index`** — the core incremental trigger described above. Returns which entities' heaps actually changed (itself + any evicted-into existing entities).
- **`POST /neighbors/index-all`** — convenience batch wrapper that calls the same logic for every entity currently in `entities`, in order — needed to bootstrap the whole current 20-row dataset without 20 manual calls, same role `/assign-shards` played in M2.
- **`GET /neighbors/{entity_id}`** — read-side lookup: cache first, `neighbor_index_read` on a miss.

New env vars for `vector-hasher` in `docker-compose.yml` (additive only, same pattern as M2): `NEIGHBOR_INDEX_K` (default `3`), `KAFKA_NEIGHBOR_TOPIC` (default `neighbor-updates`), `NEIGHBOR_CACHE_SIZE` (default `32`).

---

## 7. Verification plan

1. **`test_heap.py`**, no I/O: pushing fewer than K entries always inserts; pushing a closer entry once full evicts the farthest and only the farthest; pushing a farther entry once full is a no-op (heap unchanged); `flatten_sorted` returns ascending-by-distance order.
2. **Bootstrap the real dataset**: `POST /neighbors/index-all` against the 20 real M1/M2 rows.
3. **The literal verify criterion, with real data**: pick an entity whose shard already has several members (e.g. shard 7's `N55528`/`N55610`/`N29120`/`N39237`), re-index one of the already-indexed entities' neighbors again or index one more new one, and confirm via direct SQL on `neighbor_index_write` that an **existing** entity's `neighbors` JSONB actually changed — i.e. eviction genuinely occurred, not just that the new entity got a list.
4. **Kafka round-trip**: confirm `neighbor_index_read` ends up matching `neighbor_index_write` for the entities that changed, proving the consumer thread actually processed real messages, not just that the producer sent them.
5. **Cache correctness**: call `GET /neighbors/{entityId}` twice (should be a cache hit the second time — verify via logging/timing), then trigger a change to that entity's neighbors and confirm the very next `GET` reflects the update rather than serving the stale cached value.
6. Log the `NEIGHBOR_INDEX_K=3` and in-process-cache-not-Redis decisions in `PROJECT_SPEC.md` §10, same habit as M1/M2.

---

## Build order (same file-by-file, explain-then-confirm process as M1/M2)

`heap.py` → `test_heap.py` → `cache.py` → `002_neighbor_index_schema.sql` → `db.py` additions → `kafka_client.py` → `neighbor_index.py` → `main.py` additions → `docker-compose.yml` env vars (additive) → run and verify for real, same rigor as M1/M2.

````

## MILESTONE_4_PLAN.md

````markdown
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

````

## MILESTONE_5_PLAN.md

````markdown
# Milestone 5 — Recommendation Service (ideation / design only)

## Context

Milestones 1-4 are done and live-verified: `entities` (Postgres, Milestone 1), the LSH-sharded `neighbor_index_write`/`neighbor_index_read` tables plus vector-hasher's read API (Milestone 2-3), and `entity_history` + Redis last-N/Bloom filter/signals (Milestone 4) are all populated by real data flowing through Kafka. Nothing yet reads all of this back out for a user. Milestone 5 is that read/serving path: `GET /recommendations?userId=` — per `PROJECT_SPEC.md` §7, "retrieval, merge, ranking scale, Bloom filter, response," verified by confirming a real user's results exclude their click history and that ordering reflects distance + like/dislike.

`recommendation-service/` does not exist on disk at all yet (confirmed by exploration) — only a stub entry already sits in `docker-compose.yml`. This is a from-scratch service, not scaffolding to fill in. This document is design-only, per explicit request — no code, no new files, until this is reviewed.

## Key design decisions

### 1. Neighbor lookup: call vector-hasher's existing HTTP endpoint, don't query `neighbor_index_read` directly

vector-hasher already exposes `GET /neighbors/{entityId}` → `{entityId, neighbors: [{entityId, distance}, ...]}`, backed by its own in-process LRU cache (`NEIGHBOR_CACHE_SIZE`) and a 404 for unknown entities. This is the same *kind* of decision already made once in this project (`Entity Upload Service → Embedding Creator: synchronous REST`, §3.1) — reuse that exact pattern (mirror `entity-upload-service`'s `RestClientConfig`/`EmbeddingCreatorClient` shape) rather than opening a second, competing reader of a table vector-hasher already owns and caches. Consequence: `docker-compose.yml` needs a new `VECTOR_HASHER_URL: http://vector-hasher:8000` env var and `vector-hasher` added to `depends_on` for `recommendation-service` (both currently missing).

**Declined alternative**: reading `neighbor_index_read` directly via JDBC from `recommendation-service`. Rejected because it duplicates a query vector-hasher already makes, bypasses its cache entirely (defeating the cache's whole stated purpose — "absorbs hot-key traffic (viral articles)", §4), and creates two independent readers of a table with one designated owner.

### 2. `entities` (title/category) and `entity_history` (seen-check): read directly via JDBC — no new endpoints on other services

Unlike neighbor data, there's no existing API for either lookup, and both are simple reference/fact-table reads. `entities` is already a shared table written by one service (`entity-upload-service`) — `recommendation-service` reading it directly is the same pattern already established (write-owner / read-elsewhere), not a new one. Building a new endpoint on `entity-interaction-service` just to answer "has user X seen entity Y" would be scope creep into an already-finished, already-verified milestone for something a two-line SQL query answers directly. This will be **the first JdbcTemplate SELECT in this codebase** (confirmed: every existing repository is upsert-only) — nothing to mirror, just plain `JdbcTemplate.query(...)`.

### 3. No Kafka in this service at all

`recommendation-service`'s docker-compose stub currently sets `KAFKA_BOOTSTRAP_SERVERS` and depends on `kafka`, but nothing in this design produces or consumes a Kafka message — Redis (kept fresh by `entity-interaction-service`'s consumer) and `neighbor_index_read` (kept fresh by vector-hasher's consumer) are both already-durable, already-fresh state by the time this service reads them. Recommend **dropping `spring-kafka` from `pom.xml`** and **dropping `KAFKA_BOOTSTRAP_SERVERS`/`kafka` from `depends_on`** in docker-compose — this service is a pure synchronous fetch-and-merge read path, which is exactly what §1's stated north star asks for ("push as much computation as possible to the write path so the serving path is just fetch-and-merge").

### 4. Retrieval → merge → rank → filter → respond, concretely

```
GET /recommendations?userId={userId}
```
1. **Seeds (Redis, with Postgres fallback)**: `LRANGE user:{userId}:lastEntities 0 -1` → ordered recent entityIds (≤ `LAST_N_SIZE`, most-recent-first). **An empty result is ambiguous — it means either a genuine cold-start user, or Redis having lost/never held its state (restart, eviction, cache flush) — and must not be treated as "no history" on its own.** On empty, fall back to the durable source: `SELECT entity_id FROM entity_history WHERE user_id = ? ORDER BY event_timestamp DESC LIMIT {LAST_N_SIZE}` (Postgres). This is exactly why `entity_history` exists as the durable record behind Redis's fast/volatile cache — Milestone 4's Kafka consumer writes both from the same batch (`InteractionEventConsumer.consume`), so Postgres is always at least as complete as Redis, and is the correct fallback rather than a second, independent guess. Only if *that* is also empty is this genuinely a cold-start user → return `{userId, recommendations: []}`.
   - **Declined enhancement**: repopulating Redis's `lastEntities` list from this Postgres fallback (self-healing the cache on read). Reasonable for later, but adds a write responsibility to a service designed as Redis-read-only everywhere else in this plan (§2/§3) — left out of this pass, noted here so it isn't silently forgotten.
2. **Neighbors (HTTP, one call per seed, in parallel)**: call vector-hasher's `GET /neighbors/{entityId}` for every seed concurrently (`CompletableFuture.allOf` or equivalent) rather than sequentially — with ≤5 seeds this is cheap, and it directly serves §1's read-latency north star. A 404 for a given seed (not yet indexed) is skipped, not fatal to the request.
3. **Merge + dedupe**: pool every returned neighbor across all seeds into one map `entityId → (minDistance, sourceSeedId)`, per §4's explicit "dedupe: keep min distance" — plus tracking *which seed* produced that minimum, needed for step 4. If the same neighbor arrives from two seeds, only the closer seed's provenance is kept (simplest reading of "keep min distance"; a candidate referenced by a farther LIKE-seed and a closer neutral-seed will use the closer/neutral one — a deliberate, documented simplification, not an oversight).
4. **Soft like/dislike scaling, with a Postgres fallback for the signal itself**: first, `EXISTS user:{userId}:signals`. If it exists, `HGETALL` it once and, for each candidate, look up its *source seed's* signal (not the candidate's own — a candidate that itself has a signal entry would always also be Bloom/history-"seen" and excluded in step 5 anyway, so checking the candidate's own signal would be dead code). If the key is missing (the same Redis-state-loss case as steps 1 and 5 — every processed entity gets a signals entry unconditionally per `InteractionEventConsumer.consume()`, so a healthy user with real seeds always has one; a missing key here means Redis lost it, not that it never existed), fetch the latest `interaction_type` per seed from `entity_history` instead — one batched query over just the (≤5) seed entityIds: `SELECT DISTINCT ON (entity_id) entity_id, interaction_type FROM entity_history WHERE user_id = ? AND entity_id = ANY(?) ORDER BY entity_id, event_timestamp DESC`. This is the same fallback shape as steps 1 and 5 (Redis-missing → confirm against the durable table `entity_history` already tracks it for free, since `entity_history.interaction_type` is written in the same consumer batch as the Redis signal) — a third instance of one consistent pattern: check existence, fall back to Postgres, let Redis self-heal on the user's next real interaction. `LIKE` → multiply distance by 0.8 (closer/better); `DISLIKE` → ×1.5 (farther/worse); `CLICK`/no signal → ×1.0. `score = 1.0 / (1.0 + adjustedDistance)` (higher is better, no clamping needed). These multipliers are placeholder constants, same spirit as Milestone 4's 10%/5% LIKE/DISLIKE simulation ratio — log them in the Defensibility Tracker as a deliberate, tunable demo choice, not a derived value.
5. **Bloom filter, gated by a key-existence check, then a single batched Postgres confirm**: first, `EXISTS user:{userId}:bloomfilter` — **a missing key must not be read as "every bit is 0, so everything is unseen."** `GETBIT` on a key that doesn't exist returns 0 for every position — bit-for-bit indistinguishable from a genuinely unseen entity. A wiped or never-built filter (the same kind of Redis state loss already handled in step 1) would therefore silently pass already-seen articles straight through, with no exception, no empty result, nothing to catch — worse than step 1's gap, since that one at least announces itself as an empty list. So: if the key exists, use the normal per-candidate `mightContain` check (duplicate `BloomFilterService`'s exact formula — `BIT_SIZE=960`, `HASH_COUNT=7`, same `MurmurHash3`, read-only, no `add`) to split candidates into "0 bit → trust as unseen, free" and "all bits set → needs confirming." If the key does *not* exist, the filter can't be trusted for *anyone* this request — every candidate goes into "needs confirming" (it self-heals automatically on this user's next real interaction, since `entity-interaction-service`'s consumer sets these bits on write; nothing here needs to rebuild it). Either way, resolve the whole "needs confirming" set with **one batched query**, not one round-trip per candidate: `SELECT entity_id FROM entity_history WHERE user_id = ? AND entity_id = ANY(?)`. This is cheap even in the worst case (bloom filter missing, whole pool needs confirming) specifically because vector-hasher's `NEIGHBOR_INDEX_K` is only 3 (`PROJECT_SPEC.md` Milestone 3 entry) — the candidate pool going into step 5 is bounded at ≤5 seeds × 3 neighbors = 15 entries before dedup, never large enough to need pagination or partial batching. Exclude anything the batch query confirms as truly seen; sort what's left by score descending; take the top `RECOMMENDATION_TOP_K`.
6. **Response**: batch-fetch `title`/`category` from `entities` for just the surviving entityIds (`WHERE entity_id = ANY(?)`), assemble `{userId, recommendations: [{entityId, title, category, score}, ...]}` in score order.

### 5. Recommendation-service sharding (§3.2's `hash(userId) % 4`) — nothing to build

Same logical-sharding-only treatment already applied to Redis (`M=8`) and the neighbor index (`N=8`) — one physical container regardless. Not worth even computing/logging a shard_id here (unlike `entity_history.shard_id`, nothing downstream ever reads a shard number for this service) — actively decided against adding a no-op `ShardUtil` duplicate just for symmetry.

## Files to create

New Maven service `recommendation-service/` mirroring `entity-interaction-service`'s layout:
- `pom.xml` — Spring Boot 3.3.4 parent, Java 21; `spring-boot-starter-web`, `spring-boot-starter-jdbc`, `postgresql` (runtime), `spring-boot-starter-data-redis`, `spring-boot-starter-test`. No `spring-kafka`, no actuator (no Kafka health to report).
- `Dockerfile` — same two-stage `maven:3.9-eclipse-temurin-21` → `eclipse-temurin:21-jre-alpine` shape as `entity-interaction-service/Dockerfile`.
- `src/main/java/com/velocity/recommendation/`:
  - `RecommendationServiceApplication.java`
  - `controller/RecommendationController.java` — `GET /recommendations?userId=`, `controller/HealthController.java`
  - `service/RecommendationService.java` — orchestrates steps 1-6
  - `client/VectorHasherClient.java` + `config/RestClientConfig.java` — mirrors `entity-upload-service`'s `EmbeddingCreatorClient`/`RestClientConfig` pattern exactly, pointed at `${vector-hasher.base-url}`
  - `service/BloomFilterReadService.java` — read-only `mightContain(userId, entityId)` **and** `exists(userId)` (the key-existence gate, step 5 — a missing key means "don't trust this filter for anyone this request," not "everything is unseen"), constants and formula duplicated from `entity-interaction-service`'s `BloomFilterService`
  - `util/MurmurHash3.java` — duplicated verbatim (no shared library between services, per existing project convention)
  - `repository/EntityLookupRepository.java` — `findByIds(List<String>)` (first SELECT in the codebase)
  - `repository/EntityHistoryLookupRepository.java` — `findSeenEntityIds(userId, List<String> candidateIds)` → `Set<String>`, one batched query covering both the Bloom-positive-confirm and Bloom-missing cases in step 5 (no per-candidate query — see step 5's sizing note); `findRecentEntityIds(userId, limit)` (Redis-empty fallback, step 1); `findLatestInteractionTypes(userId, List<String> seedEntityIds)` → `Map<String, InteractionType>`, one batched `SELECT DISTINCT ON (entity_id) ...` query (signals-missing fallback, step 4)
  - `dto/` — `NeighborEntry`, `RecommendationItem`, `RecommendationResponse` records
  - No `RedisConfig.java` needed — `StringRedisTemplate` is auto-configured by Spring Boot once `spring.data.redis.host/port` are set; unlike `entity-interaction-service`, nothing here needs a custom Lua script bean.
- `src/main/resources/application.yml` — datasource (Postgres), `spring.data.redis.host/port`, `vector-hasher.base-url`, `redis.last-n-size`, `recommendation.top-k` (default 10).
- `docker-compose.yml` edits: add `VECTOR_HASHER_URL`/equivalent env + `vector-hasher` to `depends_on`; drop `KAFKA_BOOTSTRAP_SERVERS` and `kafka` from `depends_on` (§3 above).

## Execution map — build order

Bottom-up: pure logic first (cheap to unit-test in isolation, no mocks needed), then I/O adapters (each testable alone against a mock), then the orchestrator that wires them together, then infra, then live verification. Mirrors how `entity-interaction-service` was actually built this session. A `mvn test` checkpoint closes out every phase before moving to the next — no phase starts on top of an unverified one.

**Phase 1 — Skeleton, buildable and runnable**
`pom.xml`, `Dockerfile`, `application.yml`, `RecommendationServiceApplication.java`, `controller/HealthController.java`.
Checkpoint: `mvn clean package` succeeds; `docker compose up -d --build recommendation-service`; `curl localhost:8083/health` responds. Nothing else exists yet — confirms the base scaffolding before any real logic goes in.

**Phase 2 — Pure logic (no Redis, no Postgres, no HTTP)**
`util/MurmurHash3.java` (duplicated verbatim, already proven correct in `entity-interaction-service`'s test suite — copy, don't re-derive); `dto/` records (`NeighborEntry`, `RecommendationItem`, `RecommendationResponse`, plus an internal candidate record carrying `entityId`/`distance`/`sourceSeedId`); the merge+dedupe+rank step (§4.3/§4.4) as one pure function taking `Map<seedId, List<NeighborEntry>>` + the signals map and returning a scored, sorted candidate list — no I/O, so it's the cheapest and most important thing to get thoroughly unit-tested before anything depends on it, same reasoning as why `UserTimelineBuilder` was built and tested standalone in Milestone 4.
Checkpoint: `mvn test` — the merge/rank logic is fully covered (min-distance-wins dedup, provenance tracking, LIKE/DISLIKE/neutral multipliers, score formula) before it's wired to anything real.

**Phase 3 — I/O adapters, each independently mockable**
- `service/BloomFilterReadService.java` — `exists`/`mightContain`, mirrors the already-tested `entity-interaction-service` formula.
- `repository/EntityLookupRepository.java` — `findByIds`.
- `repository/EntityHistoryLookupRepository.java` — `findSeenEntityIds` (batched) and `findRecentEntityIds` (Redis-empty fallback).
- `client/VectorHasherClient.java` + `config/RestClientConfig.java` — mirrors `entity-upload-service`'s pattern; handle 404 as "skip this seed," not an error.
- small Redis readers for `lastEntities` (LRANGE) and `signals` (HGETALL) — thin enough they may just live as methods on the orchestrator rather than needing their own classes; decide while writing it, not upfront.
Checkpoint: `mvn test` — each adapter tested alone against a mocked `StringRedisTemplate`/`JdbcTemplate`/`RestClient`, same style as `BloomFilterServiceTest`/`EntityHistoryRepositoryTest`/`InteractionEventProducerTest` from Milestone 4.

**Phase 4 — Orchestration**
`service/RecommendationService.java` wires Phases 2+3 together in the order fixed by §4 (seeds w/ fallback → parallel neighbor fetch → merge/rank (Phase 2's pure function) → Bloom-gated batched filter → top-K → entity lookup → response); `controller/RecommendationController.java` — thin, delegates only.
Checkpoint: `mvn test` — orchestrator tested with every dependency mocked (mirrors `InteractionEventConsumerTest`'s style), covering the cold-start-empty, Redis-fallback-used, and Bloom-missing-fallback-used branches explicitly, not just the happy path.

**Phase 5 — Infra wiring**
`docker-compose.yml`: add `VECTOR_HASHER_URL` + `vector-hasher` to `depends_on`; drop `KAFKA_BOOTSTRAP_SERVERS`/`kafka` (§3).
Checkpoint: full `mvn test` suite green, then `docker compose up -d --build recommendation-service`.

**Phase 6 — Live verification**
Exactly the "Verification" section below, run against the real stack and the real users (`U80234`/`U91836`) already seeded earlier this session.

## Verification (once this design is approved and actually implemented)

1. `mvn test` (Docker Java 21, same as Milestone 4) — unit tests per class, mocking `StringRedisTemplate`/`JdbcTemplate`/the vector-hasher `RestClient`, same style as the 47 already in `entity-interaction-service`.
2. `docker compose up -d --build recommendation-service` alongside the already-running stack.
3. Pick a real user already replayed in this session (e.g. `U80234` or `U91836`, both already have real Redis/Postgres state from Milestone 4's verification) and hit `GET /recommendations?userId=...`; confirm none of the returned `entityId`s appear in that user's `entity_history` rows, and that ordering is consistent with distance + any recorded LIKE/DISLIKE signal.
4. Specifically test the Redis-fallback path (step 1): for a user with real `entity_history` rows, manually clear just their Redis key (`redis-cli DEL user:{userId}:lastEntities`, simulating lost cache state without touching Postgres) and confirm `GET /recommendations` still returns sensible results sourced from Postgres, not an empty list.
5. Specifically test the wiped-Bloom-filter path (step 5) — the more dangerous one, since it fails silently rather than visibly: pick a user whose candidate pool (from their real last-N/neighbors) includes at least one entity already in their `entity_history`, confirm it's correctly excluded with the Bloom filter intact, then `redis-cli DEL user:{userId}:bloomfilter` and repeat the same request — confirm that already-seen entity is *still* excluded (via the batched Postgres fallback), not silently let back through.
6. Specifically test the wiped-signals path (step 4): for a user whose seeds include at least one real LIKE or DISLIKE (not just CLICK), confirm the ranking reflects that scaling with the signals hash intact, then `redis-cli DEL user:{userId}:signals` and repeat the same request — confirm the same LIKE/DISLIKE scaling still applies (sourced from `entity_history.interaction_type` via the batched fallback), not silently degraded to neutral (×1.0) for that seed.
7. Log the placeholder ranking constants (step 4) and the min-distance-provenance simplification (step 4/5 boundary) in `PROJECT_SPEC.md` §10.

````

## PROGRESS.md

````markdown
# Progress Tracker

Update this file at the end of every milestone, before running `/compact` or ending the session. This is what tells a fresh Claude Code session (or a fresh you) exactly where things stand — don't rely on chat history to remember.

## Status

| Milestone | Status | Notes |
|---|---|---|
| 1. Entity Upload + Embedding pipeline | Done — verified | A/B/C/D all built and verified end-to-end with real MIND-small data (20-row ingest, not yet full ~51K-row run) |
| 2. Vector hashing (LSH) + shard assignment | Done — verified | vector-hasher built and verified end-to-end with real embeddings + real ingested Postgres data |
| 3. Neighbor Index write-side + Kafka resync | Done — verified | Built inside vector-hasher (no new service); real bootstrap + eviction + Kafka round-trip + cache invalidation all verified against real data |
| 4. Entity Interaction ingestion | Not started | |
| 5. Recommendation Service (serving path) | Not started | |

Status values: `Not started` / `In progress` / `Done — verified` / `Blocked`.

## Session log

Add one entry per session, most recent first.

```
Date: 2026-07-21
Milestone worked on: 3. Neighbor Index write-side + Kafka resync

What was completed:
- Built inside the existing vector-hasher service, per the milestone plan's
  resolved scope (no new service — docker-compose.yml has no second stub,
  and PROJECT_SPEC.md §9 already labels the directory "LSH + neighbor
  computation").
- lsh.py: added cosine_distance(v1, v2) and resolve_candidate_shard_ids(...)
  (the same-bucket-then-adjacent-fallback logic, pulled out of main.py's
  /candidates handler into a reusable, pure function).
- heap.py: new pure max-heap module — push_with_eviction (O(log k) insert/
  evict via negated distances in a min-heap) and flatten_sorted. Covered by
  6 unit tests in test_heap.py (up from an initial 4 — 2 more added after
  the duplicate-entry bug below).
- cache.py: new manual OrderedDict-based LRU (get/set/invalidate) — not
  functools.lru_cache, since single-key invalidation is required.
- database/init/002_neighbor_index_schema.sql: new neighbor_index_write /
  neighbor_index_read tables, applied to the live Postgres (container had
  already run once from M1/M2, so this was applied manually via psql rather
  than relying on docker-entrypoint-initdb.d's fresh-volume-only behavior).
- db.py: added fetch_candidates_with_vectors, fetch_vector, fetch_shard_id
  (already existed), fetch_neighbors_write, save_neighbors,
  save_neighbors_read, fetch_neighbors_read.
- kafka_client.py: new — create_producer/send_neighbor_update (synchronous,
  keyed by entityId), run_consumer (blocking loop, meant to run on a
  background thread).
- neighbor_index.py: new orchestrator — index_entity(...) builds the target
  entity's own list, then does the reverse eviction check against every
  candidate, saving + publishing only whatever actually changed.
- main.py: added POST /neighbors/{entityId}/index, POST /neighbors/index-all,
  GET /neighbors/{entityId}; started the Kafka consumer on a background
  thread from the existing lifespan handler; refactored /candidates to call
  the new resolve_candidate_shard_ids instead of its own inline copy of the
  same logic.
- requirements.txt: added kafka-python==2.0.2.
- docker-compose.yml: added NEIGHBOR_INDEX_K, KAFKA_NEIGHBOR_TOPIC,
  NEIGHBOR_CACHE_SIZE env vars to vector-hasher (additive only).

What was verified (and how):
- Unit tests: all 11 (5 lsh.py + 6 heap.py) passed locally, no DB/Docker
  needed.
- Real bootstrap: POST /neighbors/index-all against the real 20-row dataset
  — {"total":20,"changed":82} (after the bug fix below; was 100 before it,
  reflecting wasted duplicate-churn).
- Literal verify criterion, with real data: confirmed via direct SQL that
  shard 7's 4 real entities (N29120, N39237, N55528, N55610) each ended up
  with exactly 3 distinct neighbors — no duplicates, genuine eviction.
- Kafka round-trip: neighbor_index_write and neighbor_index_read compared
  directly via SQL for all 20 entities — zero mismatches.
- Cache correctness, end to end with one real new data point: added one
  more real article via a genuine embedding-creator call ("Kate Middleton
  Royal Style Through the Decades", inserted as N90001, real shard = 7 via
  /assign-shards). GET /neighbors/N55610 twice returned identical results
  (cache hit). POST /neighbors/N90001/index then genuinely evicted N29120
  (distance 0.922) in favor of N90001 (distance 0.336) from N55610's list —
  confirmed via the "changed" response and direct SQL. The very next GET
  immediately reflected the new neighbor, not the stale cached one —
  invalidation confirmed working, not just assumed. N90001 was left in
  place afterward (not cleaned up) per instruction.
- Real bug found and fixed during this verification (not anticipated by the
  original plan): push_with_eviction had no check for whether an entity_id
  was already present in the heap, so an entity touched by two separate
  index_entity() calls (once as itself, once as another entity's candidate)
  could get inserted twice, wasting a heap slot and evicting a genuinely
  different neighbor to make room for a duplicate. Fixed by checking heap
  membership by entity_id before inserting/evicting; covered by 2 new
  regression tests; re-verified live against a truncated-and-rebootstrapped
  neighbor_index_write/read (confirmed zero duplicates afterward).
- Hit an unrelated environment issue while starting Postgres: a native
  (non-Docker) Postgres service was already bound to host port 5432 from
  before this session. Resolved by stopping that native service (user's own
  action, since sudo needed an interactive password this environment
  couldn't provide) rather than changing docker-compose.yml's port mapping.

What's left / known issues:
- Milestones 4 and 5 (Entity Interaction ingestion, Recommendation Service)
  not started.
- The Kafka consumer thread has no supervision/graceful shutdown — logged
  as a Defensibility Tracker entry, not a bug.
- N90001 (the synthetic verification article) remains in the entities
  table and its neighbor-index rows — left in place per instruction, not
  cleaned up. Worth remembering this makes the dataset 21 rows, not 20,
  for anyone relying on that exact count later.

Any new entries added to PROJECT_SPEC.md §10 (Defensibility Tracker)? Yes — 4
entries (NEIGHBOR_INDEX_K=3 rationale, in-process-cache-not-Redis rationale,
Kafka consumer thread has no supervision, and the push_with_eviction
duplicate-entry bug found + fixed during verification).
```

```
Date: 2026-07-19
Milestone worked on: 2. Vector hashing (LSH) + shard assignment

What was completed:
- New vector-hasher service (Python/FastAPI): lsh.py (pure hashing math —
  generate_hyperplanes, compute_hash, adjacent_shard_ids), db.py (Postgres
  access — fetch_all_vectors, update_shard_ids, fetch_shard_id,
  fetch_candidates), main.py (GET /health, POST /hash, POST /assign-shards,
  GET /candidates/{entityId}), test_lsh.py (5 unit tests, all pure/no I/O),
  requirements.txt, Dockerfile, .dockerignore.
- Added LSH_SEED/LSH_NUM_HYPERPLANES/NUM_SHARDS/EMBEDDING_DIM env vars to
  vector-hasher's already-present docker-compose.yml stub (additive only).
- Cleaned up unrelated stray Docker state from a prior project (ContentPulse):
  removed its stopped kafka/redis/postgres containers and their 3 data
  volumes, which were colliding with this project's container names.

What was verified (and how):
- Unit tests: all 5 passed locally (no DB/Docker needed) — determinism, fixed-
  seed hyperplane reproducibility, near-duplicate vectors landing in the same
  bucket, opposite vectors producing fully complementary hash_bits, and
  adjacent_shard_ids wraparound. Results + descriptions written to
  vector-hasher/tests/README.md.
- Built and ran vector-hasher as a real container; GET /health returned 200.
- Real end-to-end LSH check reusing Milestone 1's exact 3 test texts (2
  similar sports headlines + 1 unrelated cooking headline) through real
  embedding-creator + vector-hasher calls. Honest result, not the clean outcome
  hoped for: the two similar articles differed in only 3 of 20 hash bits (85%
  agreement) but landed in different, non-adjacent shards (0 and 2), because
  one of those 3 differing bits happened to be one of the 3 low-order bits
  that decide shard_id (= hash_value % 8). Logged as a Defensibility Tracker
  entry with the concrete numbers rather than re-picking test text to hide it.
- Ran POST /assign-shards against the real 20 rows already in Postgres from
  Milestone 1: {"total":20,"updated":20}. Verified directly via SQL that
  shard_id is no longer uniformly 0 and is spread across shards 0-5 and 7
  (shard 6 empty, expected with only 20 sparse rows).
- Ran GET /candidates/N55528 (a real lifestyleroyals article) and confirmed
  N55610 (same subcategory, same shard) appears in the result. Also confirmed,
  honestly, that the same-bucket count (3) was below MIN_CANDIDATES=5, so the
  adjacent-bucket fallback fired and pulled in 5 more, less-related articles
  from shard 0 alongside it — expected given only 20 total rows exist, and a
  real demonstration of why bucket retrieval alone isn't a final ranking.

What's left / known issues:
- No cosine-similarity re-ranking within a candidate set yet — /candidates
  returns everyone in the searched bucket(s), unordered. That's explicitly
  Milestone 3's job (Neighbor Index), not this milestone's.
- The mod-8 shard compression (3 of 20 bits actually used) is a known,
  documented tradeoff, not a bug — see PROJECT_SPEC.md §10 for the concrete
  real-data example and reasoning.
- Full-scale /assign-shards (~51K rows, once Milestone 1's full ingest is
  eventually run) has not been exercised — only the 20-row dataset.

Any new entries added to PROJECT_SPEC.md §10 (Defensibility Tracker)? Yes — 1 entry
(mod-8 bucket compression, with the real sports_1/sports_2 bit-level example).
```

```
Date: 2026-07-19
Milestone worked on: 1. Entity Upload + Embedding pipeline (Deliverable D — closing it out)

What was completed:
- Sourced the actual MIND-small dataset (train + dev). The official msnews.github.io
  links now point to a Hugging Face mirror (huggingface.co/datasets/yjw1029/MIND);
  the original Microsoft blob-storage links are confirmed dead (409 - public access
  no longer permitted), matching PROJECT_SPEC.md's own warning that those expire.
- Verified news.tsv's real structure against MindNewsTsvParser's assumptions: all
  51,282 train rows and 42,416 dev rows have exactly 8 tab-separated columns (no
  short/truncated rows at all); 2,666 rows (~5%) have genuinely blank abstracts,
  which the parser already converts to null correctly. Risk #1 from the original
  plan is now resolved with real data, not just defensive code.
- Moved the dataset into data/mind-small/{train,dev}/ (was briefly loose at the
  repo root). Recreated .gitignore (deleted during the earlier clean-slate reset,
  never rebuilt since).
- Added the planned docker-compose.yml change for entity-upload-service:
  MIND_NEWS_TSV_PATH env var + a read-only volume mount of ./data/mind-small.
  Additive only — no existing lines touched.

What was verified (and how):
- Brought up postgres + embedding-creator + entity-upload-service together;
  confirmed the volume mount actually worked via `docker exec ... ls` showing
  the real news.tsv/behaviors.tsv inside the container.
- Ran a real POST /ingest?limit=20 against actual MIND-small data end-to-end:
  {"total":20,"succeeded":20,"failed":0,"failedIds":[]}.
- Verified directly in Postgres, not just the HTTP response: 20 rows present,
  every vector exactly 1536 bytes, titles/categories matching the real news.tsv
  content exactly (e.g. N19639 "50 Worst Habits For Belly Fat", N55528 "The
  Brands Queen Elizabeth...").
- Idempotency was not re-verified through the live system this session — relying
  on the manual SQL-level upsert proof from the previous session.

What's left / known issues:
- Full-scale ingest (~51,282 rows, no limit) has not been run — only 20 rows
  have gone through the real system, so actual throughput/timing and true
  multi-batch behavior (batch size 100) remain unobserved in practice.
- MINDlarge_train.zip was briefly downloaded by mistake before switching to
  MIND-small to keep this project's stated scope.

Any new entries added to PROJECT_SPEC.md §10 (Defensibility Tracker)? Yes — 1 entry
(verified correctness at small scale via ?limit=20, deferred the full 51K-row run).
```

```
Date: 2026-07-19
Milestone worked on: 1. Entity Upload + Embedding pipeline

What was completed:
- Deliverable A (Embedding Creator): app/main.py (FastAPI + sentence-transformers
  all-MiniLM-L6-v2, lifespan-loaded model), requirements.txt, tests/test_main.py,
  Dockerfile, .dockerignore.
- Deliverable B (Entity Upload Service): full Maven project — NewsArticle,
  MindNewsTsvParser, EmbedRequest/EmbedResponse, EmbeddingCreatorClient,
  EntityRepository, IngestionService, HealthController, IngestController,
  EntityUploadServiceApplication, pom.xml, application.yml, RestClientConfig,
  Dockerfile.
- Deliverable C: database/init/001_schema.sql — entities table, vector as BYTEA,
  shard_id default 0.
- Deliverable D: deliberately deferred — no MIND dataset downloaded yet, no
  docker-compose.yml volume mount added (on hold per instruction).

What was verified (and how):
- Embedding Creator: pytest (3 tests) passed locally; real cosine similarity
  check gave 0.679 for two similar sports headlines vs -0.04 for an unrelated
  cooking headline. Also built + ran as a live Docker container; hit real
  GET /health and POST /embed over the network (not just the in-process
  TestClient).
- Entity Upload Service: Docker build succeeded (11 source files, BUILD SUCCESS)
  after fixing a missing EntityUploadServiceApplication.java (main class was
  never recreated after an earlier full reset). Ran as a live container against
  a live Postgres container; GET /health returned 200. Confirmed genuine DB
  connectivity via Postgres's own pg_stat_activity — matched the app's HikariCP
  connection backend_start timestamp against its own application log's startup
  timestamp, rather than just trusting "no startup errors."
- DB schema: verified against a live Postgres — \d entities matches the design
  exactly; proved the INSERT ... ON CONFLICT (entity_id) DO UPDATE upsert
  EntityRepository uses actually works (re-inserting the same entity_id updated
  the row in place, count stayed at 1).

What's left / known issues:
- Deliverable D: download MIND-small, place under data/mind-small/, add the
  still-pending docker-compose.yml volume mount + MIND_NEWS_TSV_PATH env var
  (additive only).
- Run a real POST /ingest against the actual dataset — this is also the first
  time EmbeddingCreatorClient's real HTTP call from entity-upload-service to
  embedding-creator will be exercised end-to-end (both containers share a
  network already, but this path itself is untested).
- MindNewsTsvParser's column-order assumptions have never been checked against
  a real downloaded news.tsv file — still an open risk (see PROJECT_SPEC.md §0).

Any new entries added to PROJECT_SPEC.md §10 (Defensibility Tracker)? Yes — 4 entries
(vector as BYTEA, batch/concurrency + retry-skip strategy, /ingest as a blocking
endpoint, JdbcTemplate over JPA).
```

````

## PROJECT_SPEC.md

````markdown
# News Recommendation Engine — Master Build Spec

This is the reference document Claude Code should read at the start of any milestone (`read PROJECT_SPEC.md and understand milestone X before proposing a plan`). `CLAUDE.md` holds only the lean, always-loaded rules — everything else, including the "why" behind each decision, lives here.

---

## 0. Prerequisites & dataset schema (verified)

**Download:** MIND-small (train + dev splits) — search "MIND dataset download" for the current mirror link; original Microsoft blob links expire periodically.

**`behaviors.tsv`** — tab-separated, 5 columns, no header row:
```
ImpressionID   UserID   Time                        History              Impressions
1              U131     11/13/2019 8:36:57 AM       N11 N22 N33          N45-1 N46-0 N47-0
```
- `History` = space-separated News IDs the user clicked *before* this impression (this is your Entity History replay source).
- `Impressions` = space-separated `NewsID-label` pairs, label `1` = clicked, `0` = shown-but-not-clicked, for *this* impression.

**`news.tsv`** — tab-separated, fields include: News ID, Category, SubCategory, Title, Abstract, URL, Title Entities, Abstract Entities. **Verify the exact column order against the actual file header during Milestone 1** — treat this as a starting assumption, not gospel, since published mirrors have shown minor structural differences.

## 1. Problem statement

Build a two-stage (retrieval → ranking) recommendation engine for news articles — the same generalizable pattern used by YouTube/TikTok/Amazon-style systems, applied to the **MIND-small** dataset (real articles, real click logs).

Two hard requirements:
- Recommend relevant articles to a user.
- Never recommend an article the user has already seen.

North star: minimize **read latency** — push as much computation as possible to the write path so the serving path is just fetch-and-merge.

---

## 2. Scope decisions — what's real vs. deliberately simplified

State these upfront in any interview — they're deliberate engineering trade-offs, not gaps.

| Decision | What's simplified | Why it's fine to simplify |
|---|---|---|
| **Sharding** | Logical sharding (a `shard_id` column/key, all shards on one Docker-Compose deployment), not physical multi-node clusters | The partitioning *algorithm* is what's being demonstrated — physically distributing across real machines is an ops concern, not a design concern. Easy to say out loud in an interview. |
| **Embedding model** | Pretrained `sentence-transformers` (frozen, no fine-tuning) | Training your own encoder is a different project; this one is about the serving/storage/retrieval pipeline around the embedding, not the embedding model itself. |
| **Vector "hashing"** | Random-hyperplane LSH (SimHash-style) instead of literal quadrant/bounding-box splitting | Real embeddings are 384-dimensional — you can't literally draw bounding boxes. Random-hyperplane LSH is the standard N-dimensional generalization of the exact same idea: similar vectors → similar hash prefix. |
| **Interaction replay** | MIND's real click logs replayed through Kafka as if arriving live, rather than a live production frontend | Gives you 100% authentic interaction data without needing real users — this was the entire point of picking MIND. |

Keep a running note of any other simplification you make during the build — see Section 10 (Defensibility Tracker).

---

## 3. Tech stack

| Component | Stack | Why |
|---|---|---|
| Entity Upload Service | Java / Spring Boot | Matches your primary stack and target SDE/Java roles; same pattern you already used in ContentPulse. |
| Entity Interaction Service | Java / Spring Boot | Same reasoning — this is a plain CRUD/event-producing service, Spring Boot's comfort zone. |
| Recommendation Service | Java / Spring Boot | The most "interview-relevant" service — this is where sharding, caching, merging, and Bloom filter logic all live. |
| Embedding Creator | Python / FastAPI + `sentence-transformers` | Python owns the ML tooling ecosystem; also strengthens your data-adjacent/Python prep story. Mirrors the FastAPI split you already used in ContentPulse. |
| Vector hashing (LSH) + neighbor computation | Python | NumPy makes hyperplane projection and cosine similarity trivial; this logic is compute-heavy, not I/O-heavy, so Python is a fine fit here. |
| Entity DB, Entity History DB, Neighbor Index (both copies) | PostgreSQL | One relational engine, multiple logical schemas/tables — keeps local setup simple. Swap for real distributed stores later if you want to extend the project. |
| Redis | Last-N cache + Bloom filter, sharded via `hash(userId) % N` | Gives you durability/persistence for free (RDB/AOF) instead of hand-rolling S3-snapshot recovery — the trade-off we discussed. |
| Kafka (KRaft mode, no Zookeeper) | Event backbone — interaction events, neighbor-index resync | Ordering-per-key, replay via offsets, decouples ingestion from durable writes. Zookeeper-based Kafka is deprecated — KRaft is the current standard. |
| Docker Compose | Orchestrates Kafka, Postgres, Redis, and all services locally | One-command local spin-up — critical for actually finishing this quickly. |

### 3.1 Inter-service communication — decided explicitly

- **Entity Upload Service → Embedding Creator:** synchronous REST (`POST /embed`). Reasoning: embedding generation for a batch ETL load doesn't need to be decoupled — it's a one-time ingestion job, not a high-throughput live path. Keep it simple.
- **Entity Interaction Service → everything downstream:** asynchronous via Kafka only (per the dual-write discussion earlier) — never a direct synchronous call to the Entity History DB or Redis from the interaction service itself.
- **Neighbor Index write-side → read-side:** asynchronous via Kafka (CDC-style resync), never a direct write to the read copy.

### 3.2 Shard counts — reconciled across layers

Pick these once, put them in one config file, and never hardcode any of them inline in service code:

| Layer | Shard key | Count (suggested) |
|---|---|---|
| Neighbor Index (write) | `simhash_bucket % N` | N = 8 |
| Neighbor Index (read) | `hash(entityId) % N` | N = 8 (can differ from write-side; kept equal here for simplicity) |
| Redis (last-N + Bloom filter) | `hash(userId) % M` | M = 8 |
| Recommendation Service instances | `hash(userId) % P` | P = 4 (fewer than Redis shards is fine — no requirement that these match) |

---

## 4. System architecture diagram

```
 MIND Dataset (title+abstract+category)
              │
              ▼
 ┌─────────────────────────┐
 │ Entity Upload Service     │  Spring Boot · reads MIND articles, registers entity
 │ (Java)                    │  Why: single point of ingestion for new entities
 └────────────┬─────────────┘
              │ POST /embed  (entityId, text)
              ▼
 ┌─────────────────────────┐
 │ Embedding Creator          │  FastAPI · sentence-transformers (384-dim)
 │ (Python)                   │  Why: pretrained model = content vector, not
 └────────────┬─────────────┘  interaction-based — fixes the MovieLens defect
              │ vector: float[384]
    ┌─────────┴─────────┐
    ▼                     ▼
┌───────────────┐  ┌───────────────┐
│ Entity DB       │  │ LSH Hasher      │  Random-hyperplane SimHash
│ entityId→vector │  │ (Python)        │  Why: N-dim generalization of
│ Why: O(1) exact │  │ hash = k bits   │  quadrant-splitting; similar
│ lookup by ID    │  └───────┬───────┘  vectors → similar hash prefix
└───────────────┘            │
                    shard_id = hash % N
                              ▼
                  ┌─────────────────────┐
                  │ Neighbor Index (write) │  Postgres, shard: vector-hash
                  │ per-entity MAX-HEAP    │  Why: O(1) peek-farthest for
                  │ (in-memory)            │  cheap eviction on insert;
                  └──────────┬───────────┘  same-shard = atomic co-update
                              │ on change → produce
                              ▼
                        ┌─────────┐
                        │  Kafka   │  key = entityId → ordering per entity
                        └────┬────┘
                              ▼
                  ┌─────────────────────┐        ┌───────────────────┐
                  │ Neighbor Index (read)  │───────▶│ Neighbor Index      │
                  │ shard: entity-hash     │        │ Cache (LRU)          │
                  │ sorted list (flat)     │        │ Why: absorbs hot-key │
                  │ Why: kills hot         │        │ traffic (viral       │
                  │ partitions from        │        │ articles)            │
                  │ similar-content        │        └───────────────────┘
                  │ clustering             │
                  └─────────────────────┘


 User ──▶ Entity Interaction Service (Java) ──▶ Kafka (key = userId)
                                                        │
                              ┌─────────────────────────┼─────────────────────────┐
                              ▼                                                   ▼
                  ┌─────────────────────┐                             ┌───────────────────┐
                  │ Entity History DB      │  Postgres, shard: userId   │ Redis (per user)     │
                  │ index: entityId,       │  Why: durable source of    │ - last-N entities     │
                  │ 2ndry idx: timestamp   │  truth; batched async      │ - Bloom filter bitmap │
                  └─────────────────────┘  writes, not per-event       │ shard: hash(userId)%N │
                                            (avoids DB as bottleneck)   │ Why: RAM speed +      │
                                                                        │ built-in durability    │
                                                                        └───────────────────┘

 User ──▶ LB ──▶ Recommendation Service (Java, shard: hash(userId)%N)
                        │
                        ├─ 1. Redis: fetch last-N entities + read Bloom filter
                        ├─ 2. Neighbor Cache/Index: fetch precomputed neighbor lists
                        ├─ 3. K-way merge sorted lists (dedupe: keep min distance)
                        ├─ 4. Apply like/dislike scaling factor (soft penalty, not hard filter)
                        ├─ 5. Bloom filter pass per candidate
                        │      → "not seen" = keep, no DB call
                        │      → "possibly seen" = confirm via Entity History DB
                        └─ 6. Return sorted, filtered top-K
```

---

## 5. End-to-end data flow — concrete object shapes

### 5.1 Ingestion: article → embedding → storage

```
Article        { entityId, title, abstract, category, subcategory }
EmbedRequest    { entityId, text: title + " " + abstract }
EmbedResponse   { entityId, vector: float[384] }
```
Stored in Entity DB as `(entityId PK, vector float[384], category, subcategory)`.

### 5.2 Vector hashing & shard assignment

```
hash_bits = [ sign(dot(vector, r_i)) for r_i in random_hyperplanes[0..k-1] ]   # k = 20
hash_value = bits_to_int(hash_bits)          # 0 .. 2^20-1
shard_id   = hash_value % NUM_SHARDS         # NUM_SHARDS = 8 (config-driven)
```
Candidates for nearest-neighbor = all entities in the same (or adjacent, if too few) bucket. Real cosine similarity is then computed only within that small candidate set.

### 5.3 Neighbor index write + Kafka resync

```
NeighborUpdate  { entityId, neighbors: [ {id, distance}, ... ] }   # full list, not a diff
```
Keyed by `entityId` on the Kafka topic — guarantees ordering for updates to the same entity's list. Read-side consumer overwrites the row wholesale on every message (idempotent, safe to replay).

### 5.4 User interaction ingestion

```
InteractionEvent { userId, entityId, interactionType: CLICK|LIKE|DISLIKE, timestamp }
```
Keyed by `userId` → all of one user's events land in order, on the recommendation server instance that owns them.

**MIND data note:** MIND only provides click/no-click signals (label `1`/`0` in `behaviors.tsv`). LIKE and DISLIKE don't exist in the raw data. For the ranking-stage scaling factor demo, simulate a small percentage of clicks as LIKE/DISLIKE (e.g., randomly assign 10% of clicks as LIKE, 5% as DISLIKE) during the replay ETL. **Log this in §10 Defensibility Tracker** — it's a deliberate extension to demonstrate the ranking pipeline, not a claim that the data is organic.

Redis state per user:
```
user:{userId}:lastEntities   → bounded list, max N=5 (ring buffer semantics)
user:{userId}:bloomfilter    → bitmap, sized via m = -(n·ln p)/(ln2)², k ≈ (m/n)·ln2
user:{userId}:signals        → { entityId: LIKE|DISLIKE }
```

### 5.5 Serving path

```
GET /recommendations?userId=55
→ 200 OK
  { userId: 55, recommendations: [ {entityId, title, category, score}, ... ] }
```

---

## 6. Data structures used — and why (kept short on purpose)

| Structure | Used where | Why this one |
|---|---|---|
| Fixed-length float array (384) | Embeddings | Direct numeric ops (cosine/dot product) need fixed dimensionality. |
| Bit string / int (LSH hash) | Vector shard key | Prefix/bit-similarity ≈ spatial similarity → enables range/mod partitioning. |
| Max-heap (in-memory) | Neighbor index, write side | O(log k) insert, O(1) peek-farthest — the exact operation needed to decide evictions on insert. |
| Flat sorted array | Neighbor index, read side | Never mutated in place, so no need to pay heap-maintenance cost on reads. |
| Bit array + k hash functions (Bloom filter) | "Has this user seen X" | O(1) fixed-size check; false positives are safe (fallback to DB), false negatives are impossible by construction. |
| Bounded ring buffer | Last-N entities | O(1) push + evict-oldest, preserves recency order without unbounded growth. |

---

## 7. Milestones — one explore → plan → code → verify cycle each

Do not combine milestones. Finish and verify one before starting the next; run `/compact` in between to keep context clean.

### Milestone 1 — Entity Upload + Embedding pipeline
**Goal:** Ingest MIND articles, generate embeddings, persist to Entity DB.
**Claude Code opening prompt (Plan Mode):**
> Read PROJECT_SPEC.md sections 3-6. I want to build Milestone 1: Entity Upload Service (Spring Boot) that reads MIND-small article data and registers entities, plus an Embedding Creator (FastAPI + sentence-transformers) that returns a 384-dim vector for a given title+abstract. Propose a plan: project structure, the API contract between the two services, the Entity DB schema, and what could go wrong. Don't write code yet.
**Verify:** feed 2 known-similar articles (same category) and 1 unrelated article; confirm embeddings exist, correct dimension, and cosine similarity is visibly higher between the similar pair.

### Milestone 2 — Vector hashing (LSH) + shard assignment
**Goal:** Implement random-hyperplane hashing, bucket assignment, candidate retrieval within a bucket.
**Verify:** the same 2 similar articles from Milestone 1 land in the same or adjacent bucket; the unrelated one does not.

### Milestone 3 — Neighbor Index write-side + Kafka resync to read-side
**Goal:** Max-heap-based neighbor list maintenance on insert, Kafka propagation, entity-hash-sharded read copy + cache.
**Verify:** insert a new article similar to an existing one; confirm the existing entity's neighbor list updates (eviction happens correctly), and the read-side copy reflects it after Kafka consumption.

### Milestone 4 — Entity Interaction ingestion
**Goal:** Replay MIND's real click logs through Kafka; consumers update Entity History DB (batched) and Redis (last-N + Bloom filter).
**Verify:** replay one user's real click history; confirm Redis last-N list and Bloom filter both reflect it correctly, and Entity History DB has the durable record.

### Milestone 5 — Recommendation Service (serving path)
**Goal:** Implement `GET /recommendations` — retrieval, merge, ranking scale, Bloom filter, response.
**Verify:** hit the endpoint for a real MIND user; confirm returned articles exclude everything in their real click history, and ordering reflects distance + any like/dislike signal correctly.

---

## 8. Claude Code operating discipline (recap, applied to this project)

- **Plan Mode first, every milestone.** Toggle with Shift+Tab. No edits until you've read and approved the plan.
- **Demand real verification.** Ask explicitly: "run the tests/build and show me the actual output" — never accept "it works" without evidence.
- **`/compact` after each milestone**, before starting the next — keeps the context budget clean instead of one sprawling session.
- **Use subagents for noisy exploration** (e.g. "explore how MIND's file format is structured" or "run the full test suite and summarize failures") so your main session's context stays focused on decisions, not raw output dumps.
- **One milestone, one PR-sized chunk of work.** Resist the urge to ask for the whole system in one prompt — this defeats the entire point of separating explore/plan/code/verify.

---

## 9. Suggested repo structure

```
news-rec-engine/
├── PROJECT_SPEC.md
├── CLAUDE.md
├── PROGRESS.md
├── docker-compose.yml           # Kafka (KRaft), Postgres, Redis, all services
├── database/
│   └── init/                    # SQL scripts auto-run by Postgres on first boot
├── entity-upload-service/       # Java/Spring Boot
├── entity-interaction-service/  # Java/Spring Boot
├── recommendation-service/      # Java/Spring Boot
├── embedding-creator/           # Python/FastAPI
├── vector-hasher/               # Python — LSH + neighbor computation
└── scripts/
    └── mind-loader/             # one-off ETL: load MIND-small into the pipeline
```

---

## 10. Defensibility tracker (keep this updated as you build)

Log every simplification, workaround, or trade-off made during implementation here — same habit you already used for your Log Anomaly Detection project's interview dossier. Format: decision → what you'd do differently at real scale → why you didn't here.

```
Example entry:
- Decision: single Postgres instance with a shard_id column, not physically separate nodes.
- At real scale: each shard_id would be a separate physical Postgres instance/cluster.
- Why not here: demonstrating the partitioning algorithm doesn't require real hardware;
  this was a deliberate scope cut for a portfolio timeline, not an oversight.
```

### Milestone 1 entries (2026-07-19)

```
- Decision: Entity DB's vector column stored as BYTEA (raw packed little-endian
  float32, 1536 bytes, no header) instead of a native float4[] array or a
  pgvector column.
- At real scale: a dedicated vector type/extension (or a separate vector store
  entirely) would likely back similarity search directly in the database.
- Why not here: this project's whole point is hand-building vector-hash sharding
  and neighbor search in application code (Milestones 2-3) — letting pgvector do
  that job would undercut the demonstration. No SQL-side vector math is ever
  needed since all of it happens in Java/Python; BYTEA avoids float4[]'s clunkier
  JDBC java.sql.Array marshaling for a value that's only ever written once and
  read back in bulk elsewhere.

- Decision: ingestion batch size fixed at 100 rows, ~6 concurrent /embed calls,
  one retry + skip-and-log per failed row (no dead-letter queue, no configurable/
  adaptive concurrency).
- At real scale: you'd want adaptive concurrency, a real dead-letter mechanism
  for failed rows, and probably a job-tracking system instead of an in-memory
  failedIds list returned in the HTTP response.
- Why not here: this is a one-time (or occasionally re-run) ETL job over a
  static ~50k-row dataset, not a high-throughput production pipeline — the
  added infrastructure isn't justified by the actual scale or usage pattern.

- Decision: POST /ingest is a synchronous, blocking HTTP call — no background
  job, job ID, or status-polling endpoint.
- At real scale: a long-running ingestion job would be kicked off asynchronously
  (e.g. @Async, a queue, or a scheduled job) with a separate status-check
  endpoint, so the caller isn't stuck waiting.
- Why not here: MIND-small's news.tsv is a static file ingested once or
  occasionally during development, not a continuously arriving feed — there's
  no operational need for the caller to get an immediate response while the
  job runs in the background.

- Decision: EntityRepository uses plain JdbcTemplate with a hand-written upsert
  SQL statement instead of Spring Data JPA.
- At real scale: a service with many entity types and relationships would
  likely benefit from JPA's abstractions.
- Why not here: this service has exactly one write query, and it specifically
  needs native Postgres upsert (INSERT ... ON CONFLICT) semantics that JPA
  doesn't map to cleanly — raw JDBC is less machinery for one well-understood
  statement, not more.
```

### Milestone 1 — Deliverable D entry (2026-07-19)

```
- Decision: verified the ingestion pipeline end-to-end using POST /ingest?limit=20
  (20 real MIND-small articles) rather than a full run against all ~51,282 rows.
- At real scale: a production rollout would need a full-volume load test to
  characterize actual throughput, memory behavior across many batches, and
  total run time before considering the pipeline production-ready.
- Why not here: 20 rows already exercises every code path that matters for
  correctness — real parsing, the real HTTP call to embedding-creator, the
  real Postgres upsert. A full run mainly adds volume and duration, not new
  correctness risk, and can be run later with zero code changes (it's the
  same endpoint, just without the ?limit param).
```

### Milestone 2 — mod-8 bucket compression, observed with real data (2026-07-19)

```
- Decision: kept shard_id = hash_value % 8 (only 3 of the 20 hash bits actually
  determine the bucket) after observing a concrete real-data case where it
  produced a non-ideal result: real embeddings for two similar sports headlines
  ("Lakers win championship game in overtime thriller" / "NBA finals game ends
  in dramatic overtime victory") differed in only 3 of 20 hash bits (85%
  agreement) — but one of those 3 differing bits happened to be one of the 3
  low-order bits that decide shard_id, so they landed in different, non-adjacent
  shards (0 and 2). An unrelated cooking headline coincidentally landed in the
  same shard as one of the sports articles. adjacent_shard_ids(0, 8) = {7,0,1}
  does not include 2, so the "adjacent bucket" fallback would not have caught
  this pair either.
- At real scale: true multi-probe LSH (querying multiple hash variants formed
  by flipping individual hyperplane bits, not just shard_id +/-1) or a larger
  NUM_SHARDS with a proper re-ranking step over full cosine similarity within
  the candidate set would recover pairs like this.
- Why not here: this is the mod-8 compression tradeoff already anticipated in
  the plan for this milestone, now confirmed against real data rather than
  just argued in the abstract. The full 20-bit hash still carries real
  similarity signal (85% agreement in this case) — production nearest-neighbor
  systems built this way always follow up bucket retrieval with an exact
  distance computation over the (small) candidate set precisely because
  bucket membership alone is an approximation, not a guarantee. That exact
  re-ranking step is explicitly Milestone 3's job (Neighbor Index), not this
  milestone's.
```

### Milestone 3 entries (2026-07-21)

```
- Decision: NEIGHBOR_INDEX_K defaults to 3, not a larger number.
- At real scale: K would typically be larger (10-50+), sized to how many
  recommendations the serving layer actually needs to show.
- Why not here: with only ~20 real rows spread across 7 populated shards
  (2-5 entities each), a larger K would mean most entities' candidate sets
  never fill up — eviction would never be genuinely exercised by real data,
  making this milestone's own stated verify criterion ("confirm eviction
  happens correctly") untestable at this data scale. K=3 is small enough
  that indexing even the current dataset genuinely exercises eviction
  (confirmed live — see PROGRESS.md). Plain env var, trivial to raise later.

- Decision: the read-side cache (cache.py) is in-process (a manual
  OrderedDict-based LRU) rather than Redis-backed.
- At real scale: with multiple horizontally-scaled vector-hasher instances,
  an in-process cache wouldn't be shared across them — you'd want Redis so
  every instance sees the same cached state.
- Why not here: vector-hasher isn't wired to Redis at all yet (Redis is
  currently only used by the Recommendation Service's serving path, per
  the architecture in §3), and adding that wiring just for this one
  milestone's cache isn't justified by what this milestone needs to
  demonstrate — cache invalidation semantics, not distributed caching.

- Decision: the Kafka consumer runs as a fire-and-forget daemon thread
  (started once in main.py's lifespan) with no graceful shutdown, no
  reconnect/retry supervision beyond kafka-python's own defaults, and no
  offset-commit monitoring.
- At real scale: you'd want a supervised consumer (auto-restart on failure,
  health-check integration, graceful shutdown that finishes in-flight
  messages before the process exits).
- Why not here: this is a local demo service with a single instance and a
  short-lived process lifetime — the added supervision machinery isn't
  justified by the actual failure modes at this scale, and a daemon thread
  correctly dies with the process either way.

- Decision (found + fixed during verification, not anticipated in the
  original plan): push_with_eviction had no check for "is this entity_id
  already in the heap" — pushing the same entity twice (which genuinely
  happens across the two different index_entity() calls that can each
  touch the same entity's list) created duplicate neighbor entries instead
  of being a no-op. Confirmed with real data (shard 7's entities each had a
  literal duplicate entry after the first bootstrap run), fixed by checking
  heap membership by entity_id before considering insert/evict, and covered
  with two new regression tests in test_heap.py. This is logged here as a
  correctness bug that was caught by real-data verification, not a
  simplification/tradeoff like the other entries above — included for the
  same reason M2's mod-8 entry was: an honest record of what verification
  actually surfaced, not just what was planned in the abstract.
```

### Milestone 4 entries (2026-07-25)

```
- Decision: entity_history's PRIMARY KEY (source_id, entity_id) does NOT
  collapse genuine repeated History entries (sourceId is "HIST-{userId}-{index}",
  one distinct row per position in the user's History list), while Redis's
  last-N ring buffer (LREM+LPUSH+LTRIM) and Bloom filter DO collapse repeats
  of the same entity for the same user — on purpose, not an oversight.
- At real scale: this asymmetry would still hold — it isn't a scale tradeoff,
  it's a correctness distinction between two different questions.
- Why: Postgres's entity_history is the permanent record of what actually
  happened — every real occurrence (real cross-session duplicate or genuine
  repeated History entry) needs to remain a distinct row. Redis's last-N and
  Bloom filter answer "what's currently recent" and "has this ever been
  seen," respectively — neither of those questions benefits from tracking
  exact repeat counts, so collapsing repeats there is correct, not lossy.
  Noted explicitly here so a future pass doesn't "fix" the two stores to
  match each other without realizing they're intentionally answering
  different questions.

- Decision: entity-interaction-service's JUnit suite (45 tests across
  util/parser/service/repository/controller) is unit-level only — Redis
  (StringRedisTemplate), the JdbcTemplate, and KafkaTemplate are all mocked
  with Mockito; there is no @SpringBootTest, no embedded Kafka, and no
  Testcontainers-backed Postgres/Redis. ReplayService is exercised against
  real temp-file TSVs (it takes plain path strings, so no Spring context is
  needed there either). pom.xml has no spring-kafka-test / testcontainers
  dependency added.
- At real scale: you'd add embedded-Kafka + Testcontainers Postgres/Redis
  integration tests that exercise the actual @KafkaListener wiring, the real
  Lua ring-buffer script, and the real ON CONFLICT upsert — none of which a
  mocked unit test can catch (e.g. a typo in the Lua script or a JDBC type
  mismatch would still pass every test in this suite).
- Why not here: every environment-required property (datasource URL, Kafka
  bootstrap servers, Redis host, MIND file paths) is a raw `${ENV_VAR}` with
  no default in application.yml, and there's no test profile — a
  @SpringBootTest would need all of that wired to even start the context.
  Given this milestone's actual logic (TSV parsing, timeline ordering,
  bit-position math, batch upsert parameter binding, per-event vs.
  per-batch call sequencing) is fully expressible as pure functions plus
  thin adapters around three well-defined client APIs, mocking those
  clients tests the real risk (this service's own logic) without paying for
  a docker-compose-dependent test environment. Kafka/Redis/Postgres wiring
  itself is exercised manually via `docker-compose up` + the milestone's
  stated verify criterion, per CLAUDE.md's build/test commands.

- Decision (found during live verification, not a code change): the local
  `redis` container had a corrupted network attachment (HostConfig said
  `velocity_default`, but `NetworkSettings.Networks` was empty — likely
  stale from being stopped/started many times over the project's life
  without ever being recreated), which made `redis` unresolvable from
  entity-interaction-service. Fixed with
  `docker compose up -d --force-recreate redis`. Not an application bug —
  noted here only because it fully masked the real verification for one
  replay attempt (see next entry) and would silently do so again for
  anyone reusing these long-lived local containers.
- At real scale: a real orchestrator (k8s, ECS) wouldn't let a container's
  network attachment silently drift from its declared config like this.
- Why not here: this is host-local Docker Compose state, not something the
  service or its tests control.

- Decision (found live, FIXED same session): while `redis` was unreachable
  above, `InteractionEventConsumer`'s batch listener didn't retry
  indefinitely — Spring Kafka's default batch error handling
  (`FailedBatchProcessor` -> `FallbackBatchErrorHandler`) retried a few
  times, then logged "Records discarded: interaction-events-4@0..18" and
  moved on. The offsets were committed past those records; the 19 events
  were gone from processing (not just delayed) until userId=U80234 was
  replayed a second time by hand. No dead-letter topic, no alerting, no
  infinite-retry/backoff config existed anywhere in KafkaConsumerConfig.
  Fix: `KafkaConsumerConfig` now builds a `DefaultErrorHandler` from an
  `ExponentialBackOff` (1s initial, x2 multiplier, capped at 30s between
  attempts, `maxElapsedTime`/`maxAttempts` left at their unlimited
  defaults) and wires it onto `batchFactory` via `setCommonErrorHandler`.
  Declined, not implemented: a dead-letter topic/recoverer (no
  poison-message case exists for this listener — every operation is
  idempotent, see the entry above — so a DLQ solves a problem this service
  doesn't have) and wrapping the value deserializer in
  `ErrorHandlingDeserializer` (a real but separate gap: a genuinely
  malformed message would still crash the consumer thread before reaching
  this error handler; different failure mode than the one observed, left
  as future work).
- Bug found while building the fix itself, also fixed: the first attempt
  used `errorHandler.setRetryListeners(record, ex, attempt) -> log.warn(...))`
  as a lambda. `RetryListener` has two `failedDelivery` overloads — a
  single-`ConsumerRecord` one (the only abstract method, so the only one a
  lambda can implement) and a default no-op `ConsumerRecords` (plural) one.
  Since `batchFactory` is a *batch* listener, `ErrorHandlingUtils.retryBatch`
  always calls the plural overload, so the lambda silently never fired —
  confirmed via `kill -3` thread dumps showing the consumer thread legitimately
  parked in retry/backoff (not stuck), while zero log lines appeared. Fixed
  by implementing `RetryListener` as an anonymous class overriding the
  `ConsumerRecords` overload explicitly.
- At real scale: you'd likely still keep the infinite-retry choice (matches
  this project's own "Kafka decouples ingestion from durable writes"
  reasoning in §3) but might add metrics/alerting on retry duration so an
  operator knows when a dependency has been down "too long," rather than
  relying on log lines alone.
- Why an infinite retry (not a DLQ) is fine here: every operation this
  listener performs is idempotent (see the entry above), so there is no
  message that can never succeed — only dependencies that are temporarily
  down. A local demo's failure modes don't include a genuinely poisoned
  `InteractionEvent`.
- Verified live, twice, against the corrected code (`docker network
  disconnect/connect velocity_default redis` to simulate the outage without
  touching host systemd services): a fresh user's batch sat retrying with
  `WARN ... Retrying interaction-events batch of N (attempt K) after: ...`
  logged on each attempt (no `Records discarded`), then — once Redis was
  reconnected, with no manual re-replay — the same batches were consumed
  successfully and `entity_history` + Redis ended up fully and correctly
  populated (96/96 rows for the test user). The dominant delay between
  attempts in this test was Lettuce's default ~60s command timeout (not the
  configured 1s/2s backoff), since the simulated outage was a live
  connection going silent rather than an instant DNS failure — worth knowing
  if this is ever demoed live, but doesn't change correctness.
```

### Milestone 5 entries (2026-07-28)

```
- Decision: `recommendation-service` calls vector-hasher's existing
  `GET /neighbors/{entityId}` over HTTP for candidate retrieval, rather than
  querying `neighbor_index_read` directly via its own JDBC connection —
  reuses vector-hasher's own read-side LRU cache and keeps that table
  single-owner. `entities` (title/category) and `entity_history` (seen-check,
  fallbacks) ARE queried directly via JDBC, since those are plain shared
  reference/fact tables with no existing service API, matching the same
  write-owner/read-elsewhere pattern `entities` already had (entity-upload
  -service writes it, this service reads it). Full reasoning in
  MILESTONE_5_PLAN.md.
- Decision: ranking uses placeholder constants — LIKE distance x0.8,
  DISLIKE x1.5, neutral/CLICK/no-signal x1.0, `score = 1/(1+adjustedDistance)`
  — same spirit as Milestone 4's 10%/5% LIKE/DISLIKE simulation ratio:
  deliberate, tunable demo values, not derived from any real ranking model.
- Decision: the merge step keeps a candidate's *minimum raw distance* across
  seeds and only that winning seed's signal is used for scaling — a
  candidate referenced by both a closer neutral seed and a farther LIKE seed
  gets no boost, since the closer/neutral seed wins provenance. A documented
  simplification (see CandidateRankerTest's
  `minDistanceWinsProvenanceEvenWhenAFartherSeedWasLiked`), not a bug.
- Decision: `recommendation-service` has three independent Redis-state-loss
  fallbacks, all one consistent shape (check existence -> fall back to a
  batched `entity_history` query -> let Redis self-heal on the user's next
  real interaction), found and added during design review before any code
  was written: (1) last-N (`lastEntities` empty -> `findRecentEntityIds`),
  (2) the Bloom filter (missing key -> batched `findSeenEntityIds` for
  every candidate instead of trusting "0 bits = unseen"), (3) signals
  (missing key -> batched `findLatestInteractionTypes` for just the seeds).
  All three live-verified against real data (see below) by wiping each
  Redis key in turn for a real user and confirming identical, correct
  results came back from Postgres instead.
- Decision: no dedicated executor for the parallel per-seed vector-hasher
  calls — `CompletableFuture.supplyAsync` on the JVM's default common pool.
  At most `LAST_N_SIZE` (5) concurrent HTTP calls per request; a dedicated
  thread pool would be configuration for its own sake at this scale.
- Decision (found while implementing, not in the original plan): the
  Bloom-gated filter is a single filter pass over the already-sorted
  candidate list (`ranked.stream().filter(...).limit(topK)`), not "split
  into free/needs-confirming lists, then recombine" as originally sketched
  — splitting and recombining loses the original score order and would
  need a second sort; filtering the one sorted list in place doesn't.

- Decision (found live, real cross-milestone gap, fixed same session):
  Milestones 1-3 had only ever been exercised against ~20-21 entities
  end-to-end. Milestone 4's replay uses real MIND behaviors.tsv data
  referencing thousands of distinct entity IDs, essentially none of which
  overlapped with the 21 already-embedded/indexed entities — so a real
  replayed user's seeds had zero neighbor data to retrieve, a data-
  availability gap rather than a Milestone 5 bug. Fixed by fully ingesting
  both `train/news.tsv` (51,282 articles) and `dev/news.tsv` (42,416
  articles) — 65,239 distinct entities after dedup — via
  `entity-upload-service`, then running vector-hasher's `/assign-shards`
  and `/neighbors/index-all` over all of them.
- Bug found and fixed during that ingestion (in already-completed
  Milestone 2/3 code, not Milestone 5): `vector-hasher`'s
  `db.update_shard_ids` built one `UPDATE ... FROM (VALUES ...)` with 2 SQL
  parameters per assignment in a single statement — fine at ~20 entities,
  but Postgres has a hard 65,535-bound-parameter limit per query, and
  65,239 entities x 2 params = 130,478 blew past it (`psycopg
  .OperationalError: number of parameters must be between 0 and 65535`).
  Fixed by batching the same UPDATE into chunks of 10,000 assignments
  (20,000 params/batch) reusing one connection, rather than one giant
  statement. Confirmed fixed: `/assign-shards` succeeded for all 65,239
  afterward.
- At real scale: neither of the two items above is a "real scale" tradeoff
  in the usual sense — they're both correctness gaps that a portfolio
  project's own necessarily-small verify-as-you-go milestones hadn't yet
  exercised at production-like data volume. Both are now fixed permanently,
  not worked around.
- Verified live end-to-end against real data for user U80234 (real MIND
  click history from Milestone 4's earlier verification): `GET
  /recommendations?userId=U80234` returned 10 real, correctly-titled/
  -categorized, score-sorted articles, none present in U80234's real
  `entity_history`. Injected one additional real interaction (`POST
  /interactions`, entityId N27132, a genuine neighbor of an existing seed)
  and confirmed: (a) N27132 was correctly excluded from recommendations
  once seen; (b) after `DEL user:U80234:bloomfilter`, N27132 stayed
  correctly excluded via the batched Postgres fallback, not silently let
  back through; (c) after `DEL user:U80234:lastEntities`, the identical
  recommendation set came back sourced from `findRecentEntityIds`; (d)
  after `DEL user:U80234:signals`, a real LIKE-boosted candidate (N11744,
  score 0.580016698609928 both before and after) kept its exact boosted
  score via `findLatestInteractionTypes`. Also observed, unprompted: the
  injected click pushed N27132 into U80234's last-N ring buffer, and its
  own (much closer, 0.39-0.51 distance) neighbors organically took over the
  top of the next call's results — the whole pipeline responding correctly
  to a real new interaction, not just a static fixture.
```

````


