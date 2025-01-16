from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import openai
from utils.WebScraper import WebScraper
from utils.store import WeaviateStore
from dotenv import load_dotenv
import os
import logging


router = APIRouter()  # Change this from FastAPI() to APIRouter()

class DocumentRequest(BaseModel):
    url: str

class SearchRequest(BaseModel):
    query: str

scraper = WebScraper()
store = WeaviateStore()
load_dotenv()

opanai_api_key = os.getenv("OPENAI_API_KEY")


@router.get("/")  # Change app to router
async def home():
    return {"message": "Hello, FastAPI is running!"}

@router.post("/create-document")
async def create_document(request: DocumentRequest):
    try:
        url = request.url
        if not url:
            raise HTTPException(status_code=400, detail="URL is required")
        
        # Scrape the content from the provided URL
        scraped_content = {}
        scraped_content["text"] = "THIS IS THE CONTENT OF DOCUMENT 4"

        # scraped_content = await scraper.scrape_url(url)
        if not scraped_content.get("text"):
            raise HTTPException(status_code=400, detail="No text content extracted")

        logging.info(f"Scraped content length: {len(scraped_content['text'])}")

        try:
            response = openai.embeddings.create(
                model="text-embedding-3-small",
                input=scraped_content["text"]
            )
            
            if not response.data or not response.data[0].embedding:
                raise HTTPException(status_code=500, detail="Failed to generate embeddings")

            embeddings = response.data[0].embedding
            logging.info(f"Generated {len(embeddings)} embeddings successfully")

        except openai.OpenAIError as e:
            logging.error(f"OpenAI API Error: {e}")
            raise HTTPException(status_code=500, detail="Error generating embeddings")

        documents = [{"text": scraped_content["text"], "title": "Scraped Document", "source_url": url}]
        embeddings_list = [embeddings]
        
        document_ids = store.store_documents(documents, embeddings_list)
        
        return {
            "message": "Document created and stored successfully!",
            "document_ids": document_ids,
            "content": scraped_content["text"][:200],
            "embeddings": embeddings[:10]
        }

    except HTTPException as e:
        raise e
    except Exception as e:
        logging.error(f"Error in create_document: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/perform-search")
async def perform_search(request: SearchRequest):
    try:
        user_query = request.query
        print("THIS IS THE QUERY", user_query)
        if not user_query:
            raise HTTPException(status_code=400, detail="Search query is required")

        response = openai.embeddings.create(
            model="text-embedding-3-small",
            input=user_query
        )
        
        if not response.data or not response.data[0].embedding:
            raise HTTPException(status_code=500, detail="Failed to generate query embeddings")
            
        query_vector = response.data[0].embedding
        similar_docs = store.search_similar(user_query)
        # similar_docs = store.search_similar_bm25(user_query)
        
        return {
            "results": similar_docs,
            # "count": len(similar_docs)
        }
        
    except openai.OpenAIError as e:
        logging.error(f"OpenAI API Error: {e}")
        raise HTTPException(status_code=500, detail="Error generating search embeddings")
    except Exception as e:
        logging.error(f"Search error: {e}")
        raise HTTPException(status_code=500, detail=str(e))