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