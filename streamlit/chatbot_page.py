from fastapi import FastAPI
import spacy
from pydantic import BaseModel
import pinecone
import os
from dotenv import load_dotenv

load_dotenv()
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")

# Initialize FastAPI app
app = FastAPI()

# Load the SpaCy model
nlp = spacy.load("en_core_web_md")

# Initialize Pinecone
pinecone.init(api_key=PINECONE_API_KEY, environment="gcp-starter")
index = pinecone.Index("yelpreviewembeddings")

# Request model
class Query(BaseModel):
    text: str

# Endpoint to create embeddings and query Pinecone
@app.post("/embeddings/")
async def query_reviews(query: Query):
    # Create embedding from the query text
    doc = nlp(query.text)
    embedding = doc.vector.tolist()

    # Query Pinecone with the generated embedding
    try:
        query_result = index.query(
            embedding,  # Pass the embedding directly
            top_k=10,  # Retrieve top 10 results
            include_metadata=True
        )

        # Check if 'matches' is in the query_result
        if 'matches' in query_result:
            matches = query_result['matches']
            return {
                'matches': [
                    {'metadata': match['metadata'], 'score': match['score']}
                    for match in matches
                ]
            }
        else:
            return {"error": "Unexpected response structure from Pinecone"}
    except Exception as e:
        return {"error": str(e)}