# from fastapi import FastAPI
# import spacy
# from pydantic import BaseModel
# import pinecone
# import os
# from dotenv import load_dotenv

# load_dotenv()
# PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")

# # Initialize FastAPI app
# app = FastAPI()

# # Load the SpaCy model
# nlp = spacy.load("en_core_web_md")

# # Initialize Pinecone
# pinecone.init(api_key=PINECONE_API_KEY, environment="gcp-starter")
# index = pinecone.Index("yelpreviewembeddings")

# # Request model
# class Query(BaseModel):
#     text: str

# # Endpoint to create embeddings and query Pinecone
# @app.post("/embeddings/")
# async def query_reviews(query: Query):
#     # Create embedding from the query text
#     doc = nlp(query.text)
#     embedding = doc.vector.tolist()

#     # Query Pinecone with the generated embedding
#     try:
#         query_result = index.query(
#             embedding,  # Pass the embedding directly
#             top_k=10,  # Retrieve top 10 results
#             include_metadata=True
#         )

#         # Check if 'matches' is in the query_result
#         if 'matches' in query_result:
#             matches = query_result['matches']
#             return {
#                 'matches': [
#                     {'metadata': match['metadata'], 'score': match['score']}
#                     for match in matches
#                 ]
#             }
#         else:
# #             return {"error": "Unexpected response structure from Pinecone"}
# #     except Exception as e:
# #         return {"error": str(e)}

# from fastapi import FastAPI
# import spacy
# from pydantic import BaseModel
# import pinecone
# import os
# from dotenv import load_dotenv
# from collections import Counter
# from datetime import datetime
# import requests

# load_dotenv()
# PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")

# # Initialize FastAPI app
# app = FastAPI()

# # Load the SpaCy model
# nlp = spacy.load("en_core_web_md")

# # Initialize Pinecone
# pinecone.init(api_key=PINECONE_API_KEY, environment="gcp-starter")
# index = pinecone.Index("yelpreviewembeddings")

# # Request model
# class Query(BaseModel):
#     text: str

# def get_yelp_business_details(business_id: str):
#     YELP_API_KEY = os.getenv("YELP_API_KEY")
#     if not YELP_API_KEY:
#         raise EnvironmentError("YELP_API_KEY not found in environment variables.")

#     url = f"https://api.yelp.com/v3/businesses/{business_id}"
#     headers = {
#         "accept": "application/json",
#         "Authorization": f"Bearer {YELP_API_KEY}"
#     }
#     try:
#         response = requests.get(url, headers=headers)
#         response.raise_for_status()  # Raises an HTTPError for bad responses
#         return response.json()
#     except requests.exceptions.RequestException as e:
#         raise HTTPException(status_code=500, detail=str(e))

# @app.post("/embeddings/")
# async def query_reviews(query: Query):
#     # Create embedding from the query text
#     doc = nlp(query.text)
#     embedding = doc.vector.tolist()

#     # Query Pinecone with the generated embedding
#     try:
#         query_result = index.query(embedding, top_k=50, include_metadata=True)  # Increased top_k for broader search

#         if 'matches' in query_result:
#             matches = query_result['matches']

#             # Count Business IDs
#             business_count = Counter(match['metadata']['BUSINESS_ID'] for match in matches)

#             # Find the most frequent Business IDs
#             max_count = max(business_count.values())
#             frequent_businesses = [bid for bid, count in business_count.items() if count == max_count]

#             # In case of a tie, prioritize by latest date
#             if len(frequent_businesses) > 1:
#                 frequent_businesses.sort(key=lambda bid: max(datetime.fromisoformat(match['metadata']['DATE']) 
#                                                             for match in matches if match['metadata']['BUSINESS_ID'] == bid), 
#                                          reverse=True)
                
#             yelp_details = []
#             for business_id in frequent_businesses[:10]:  # Limit to top 10 results
#                 try:
#                     details = get_yelp_business_details(business_id)
#                     yelp_details.append(details)
#                 except HTTPException as http_err:
#                     # Handle error (e.g., log it, return partial data with error info, etc.)
#                     # For simplicity, we're just continuing to the next business ID
#                     continue

#         return {"business_details": yelp_details}

#     #         # Return only Business IDs
#     #         return {"business_ids": frequent_businesses[:10]}  # Limit to top 10 results
#     #     else:
#     #         return {"error": "Unexpected response structure from Pinecone"}

#     # except Exception as e:
#     #     return {"error": str(e)}

from fastapi import FastAPI, HTTPException
import spacy
from pydantic import BaseModel
import pinecone
import os
from collections import Counter
from datetime import datetime
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
YELP_API_KEY = os.getenv("YELP_API_KEY")

if not PINECONE_API_KEY:
    raise EnvironmentError("PINECONE_API_KEY not found in environment variables.")
if not YELP_API_KEY:
    raise EnvironmentError("YELP_API_KEY not found in environment variables.")

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

# Function to get business details from Yelp API
def get_yelp_business_details(business_id: str):
    url = f"https://api.yelp.com/v3/businesses/{business_id}"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {YELP_API_KEY}"
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raises an HTTPError for bad responses

        # Extract the necessary data from the Yelp API response
        data = response.json()
        simplified_data = {
            "name": data.get("name"),
            "image_url": data.get("image_url"),
            "is_closed": data.get("is_closed"),
            "url": data.get("url"),
            "phone": data.get("phone"),
            "review_count": data.get("review_count"),
            "categories": [category["title"] for category in data.get("categories", [])],
            "rating": data.get("rating"),
            "display_address": ", ".join(data.get("location", {}).get("display_address", [])),
            "price": data.get("price"),
            "transactions": data.get("transactions", [])
        }
        return simplified_data
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint to create embeddings and query Pinecone
@app.post("/embeddings/")
async def query_reviews(query: Query):
    # Create embedding from the query text
    doc = nlp(query.text)
    embedding = doc.vector.tolist()

    # Query Pinecone with the generated embedding
    try:
        query_result = index.query(embedding, top_k=50, include_metadata=True)

        if 'matches' in query_result:
            matches = query_result['matches']

            # Count Business IDs
            business_count = Counter(match['metadata']['BUSINESS_ID'] for match in matches)

            # Find the most frequent Business IDs
            max_count = max(business_count.values())
            frequent_businesses = [bid for bid, count in business_count.items() if count == max_count]

            # In case of a tie, prioritize by latest date
            if len(frequent_businesses) > 1:
                frequent_businesses.sort(key=lambda bid: max(datetime.fromisoformat(match['metadata']['DATE']) 
                                                            for match in matches if match['metadata']['BUSINESS_ID'] == bid), 
                                         reverse=True)

            # Get details from Yelp for the most frequent businesses
            yelp_details = []
            for business_id in frequent_businesses[:10]:  # Limit to top 10 results
                try:
                    details = get_yelp_business_details(business_id)
                    yelp_details.append(details)
                except HTTPException as http_err:
                    # Handle error (e.g., log it, return partial data with error info, etc.)
                    continue

            return {"business_details": yelp_details}
        else:
            return {"error": "Unexpected response structure from Pinecone"}

    except Exception as e:
        return {"error": str(e)}


