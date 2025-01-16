import weaviate
from weaviate.classes.init import Auth
from weaviate.classes.config import Configure
from typing import Dict, List, Any
import logging
import os
from datetime import datetime
import requests
import json

class WeaviateStore:
    def __init__(self):
        # Initialize Weaviate client for cloud connection
        weaviate_url = os.environ["WEAVIATE_URL"]
        weaviate_api_key = os.environ["WEAVIATE_API_KEY"]
        self.client = weaviate.connect_to_weaviate_cloud(
            cluster_url=weaviate_url,
            auth_credentials=Auth.api_key(weaviate_api_key),
            headers={"X-OpenAI-Api-Key": os.getenv("OPENAI_API_KEY")}
        )
        
        # Define the collection schema if it doesn't exist
        self._create_collection()

    def _create_collection(self):
        """Create a collection for storing maritime articles if it doesn't exist"""
        try:
            # Check if the collection exists using get_collection
            try:
                existing_collection = self.client.collections.get("MaritimeArticle")
                logging.info("Collection 'MaritimeArticle' already exists.")
                return
            except weaviate.exceptions.WeaviateCollectionNotFoundException:
                # Collection doesn't exist, so create it
                collection_schema = {
                    "class": "MaritimeArticle",
                    "properties": [
                        {"name": "title", "dataType": ["text"]},
                        {"name": "content", "dataType": ["text"]},
                        {"name": "url", "dataType": ["text"]},
                        {"name": "mainTopics", "dataType": ["text[]"]},
                        {"name": "keyEvents", "dataType": ["text[]"]},
                        {"name": "author", "dataType": ["text"]},
                        {"name": "locations", "dataType": ["text[]"]},
                        {"name": "publicationDate", "dataType": ["date"]},
                        {"name": "maritimeTerms", "dataType": ["text[]"]},
                        {"name": "lastVerified", "dataType": ["date"]}
                    ],
                    "vectorIndexConfig": {
                        "distance": "cosine"
                    },
                    "vectorizer": "none"  # We'll provide vectors manually
                }

                self.client.schema.create_class(collection_schema)
                logging.info("Collection 'MaritimeArticle' created successfully.")
        except Exception as e:
            logging.error(f"Error creating collection: {e}")
            raise

    def store_documents(self, documents: List[Dict[str, Any]], embeddings_list: List[List[float]]) -> List[str]:
        """
        Store multiple documents and their embeddings in Weaviate using dynamic batch processing.
        
        Args:
            documents: List of document metadata dictionaries
            embeddings_list: List of embedding vectors corresponding to each document
        
        Returns:
            List[str]: List of UUIDs for the stored documents
        """
        try:
            document_ids = []  # List to store UUIDs of the stored documents

            with self.client.batch.dynamic() as batch:  # Dynamically batch documents
                for doc, embedding in zip(documents, embeddings_list):
                    # Prepare the properties object
                    properties = {
                        "title": doc.get("title", "Untitled"),
                        "content": doc.get("text", ""),
                        "url": doc.get("source_url", ""),
                        "mainTopics": doc.get("main_topics", "").split(",") if doc.get("main_topics") else [],
                        "keyEvents": doc.get("key_events", "").split(",") if doc.get("key_events") else [],
                        "author": doc.get("author", "Unknown"),
                        "locations": doc.get("locations", "").split(",") if doc.get("locations") else [],
                        "publicationDate": doc.get("publication_date", datetime.now().isoformat()),
                        "maritimeTerms": doc.get("maritime_terms", "").split(",") if doc.get("maritime_terms") else [],
                        "lastVerified": doc.get("last_verified", datetime.now().isoformat())
                    }

                    # Add document and its embedding to the batch
                    

                    result = batch.add_object(
                        properties=properties,
                        # class_name="MaritimeArticle",
                        collection="MaritimeArticle",
                        vector=embedding  # Add the embedding vector
                    )
                    
                    # Collect the UUID of the stored document
                    document_ids.append(result)

            logging.info(f"Successfully stored {len(documents)} documents in Weaviate using dynamic batch.")
            
            # Return the list of document UUIDs
            return document_ids

        except Exception as e:
            logging.error(f"Error storing documents in batch: {e}")
            raise

    def search_similar(self, query: str, limit: int = 5) -> dict:
        """
        Search for similar documents using text similarity
        
        Args:
            query: The search query string
            limit: Maximum number of results to return
            
        Returns:
            dict: Search results with similarity metrics
        """
        try:
            maritime_articles = self.client.collections.get("MaritimeArticle")
            
            result = maritime_articles.query.near_text(
                query=query,
                limit=limit,
                return_metadata=['distance', 'certainty', 'score']  
            )
            # print(json.dumps(result, indent=4))
            return result

        except Exception as e:
            logging.error(f"Error searching documents: {e}")
            raise


    #KEY WORD SEARCH NOT WORKING
    def search_similar_bm25(self, query: str, limit: int = 5) -> dict:
        """
        Search for similar documents using BM25 (keyword-based) similarity.
        
        Args:
            query: The search query string (keywords)
            limit: Maximum number of results to return

        Returns:
            dict: Search results with similarity scores
        """
        try:
            maritime_articles = self.client.collections.get("MaritimeArticle")
            
            result = maritime_articles.query.bm25(
                query=query,
                limit=limit,
                return_metadata=['score', 'explain_score']
            )

            return result

        except Exception as e:
            logging.error(f"Error searching documents using BM25: {e}")
            raise


    def search_hybrid(self, query: str, alpha: float = 0.5, limit: int = 5) -> dict:
        """
        Perform a hybrid search combining vector and keyword search
        
        Args:
            query: The search query string
            alpha: Balance between vector (1.0) and keyword (0.0) search. Default 0.5
            limit: Maximum number of results to return
            
        Returns:
            dict: Search results with combined similarity scores
        """
        try:
            maritime_articles = self.client.collections.get("MaritimeArticle")
            
            result = maritime_articles.query.hybrid(
                query=query,
                alpha=alpha,  # Balance between vector and keyword search
                limit=limit,
                return_metadata=["score", "explain_score", "distance", "certainty"]
            )
            
            return result

        except Exception as e:
            logging.error(f"Error performing hybrid search: {e}")
            raise
    def get_document_embeddings(self) -> List[Dict[str, Any]]:
        """
        Retrieve all document IDs and their corresponding embeddings.
        
        Returns:
            List[Dict[str, Any]]: List of dictionaries containing document IDs and their embeddings
        """
        try:
            maritime_articles = self.client.collections.get("MaritimeArticle")
            
            # Query all documents with their vectors
            result = maritime_articles.query.fetch_objects(
                limit=10000,  # Adjust based on your needs
                include_vector=True
            )
            
            # Format the response
            embeddings_data = []
            for obj in result.objects:
                embeddings_data.append({
                    "document_id": obj.uuid,
                    "vector": obj.vector
                })
                
            return embeddings_data

        except Exception as e:
            logging.error(f"Error retrieving embeddings: {e}")
            raise

    def get_document_contents(self) -> List[Dict[str, Any]]:
        """
        Retrieve all document IDs and their corresponding text content.
        
        Returns:
            List[Dict[str, Any]]: List of dictionaries containing document IDs and content
        """
        try:
            maritime_articles = self.client.collections.get("MaritimeArticle")
            
            # Query all documents
            result = maritime_articles.query.fetch_objects(
                limit=10000,  # Adjust based on your needs
                additional_properties=[
                    "content",
                    "title",
                    "url",
                    "publicationDate"
                ]
            )
            
            # Format the response
            documents_data = []
            for obj in result.objects:
                documents_data.append({
                    "document_id": obj.uuid,
                    "title": obj.properties.title,
                    "content": obj.properties.content,
                    "url": obj.properties.url,
                    "publication_date": obj.properties.publicationDate
                })
                
            return documents_data

        except Exception as e:
            logging.error(f"Error retrieving document contents: {e}")
            raise

    def clear_collection(self) -> bool:
        """
        Remove all documents from the MaritimeArticle collection.
        
        Returns:
            bool: True if successful, raises an exception otherwise
        """
        try:
            maritime_articles = self.client.collections.get("MaritimeArticle")
            
            # Delete all objects in the collection
            where_filter = {
                "path": ["content"],
                "operator": "NotEqual",
                "valueText": "THIS_IS_AN_IMPOSSIBLE_STRING_TO_MATCH"  # This ensures all documents match
            }
            
            result = maritime_articles.data.delete_many(
                where=where_filter
            )
            
            logging.info(f"Successfully cleared all documents from the collection")
            return True

        except Exception as e:
            logging.error(f"Error clearing collection: {e}")
            raise


    def print_results(self, results):
        """
        Formats the query results into a dictionary and prints them.
        
        Args:
            results (QueryReturn): The raw search result object.
        """
        try:
            print("\nFormatted Results:\n")
            
            # Loop through the objects in the results
            for idx, item in enumerate(results.objects, start=1):
                print(f"\nResult {idx}:")
                print("-" * 30)
                
                # Extract metadata and properties
                metadata = item.metadata
                properties = item.properties

                # Print out each relevant field from metadata
                print(f"UUID: {item.uuid}")
                print(f"Distance: {metadata.distance}")
                print(f"Certainty: {metadata.certainty}")
                print(f"Score: {metadata.score}")

                # Print out the document properties
                print(f"Title: {properties['title']}")
                print(f"Content: {properties['content']}")
                print(f"URL: {properties['url']}")
                print(f"Publication Date: {properties['publicationDate']}")
                print(f"Author: {properties['author']}")
                print(f"Last Verified: {properties['lastVerified']}")
                print("-" * 30)

        except Exception as e:
            print(f"Error while processing results: {e}")


if __name__ == "__main__":
    import json
    from dotenv import load_dotenv
    
    # Load environment variables
    load_dotenv()
    
    # Initialize the store
    store = WeaviateStore()
    
    def print_separator():
        print("\n" + "="*50 + "\n")
    
    while True:
        print("\nWeaviate Store Management Console")
        print("1. View Document Embeddings")
        print("2. View Document Contents")
        print("3. Clear Collection")
        print("4. Search Documents (Vector)")
        print("5. Search Documents (BM25)")
        print("6. Search Documents (Hybrid)")
        print("7. Exit")
        
        choice = input("\nEnter your choice (1-7): ")
        
        try:
            if choice == "1":
                embeddings = store.get_document_embeddings()
                print_separator()
                print(f"Found {len(embeddings)} documents")
                for doc in embeddings:
                    print(f"Document ID: {doc['document_id']}")
                    print(f"Vector (first 5 dimensions): {doc['vector'][:5]}\n")
                
            elif choice == "2":
                documents = store.get_document_contents()
                print_separator()
                print(f"Found {len(documents)} documents")
                for doc in documents:
                    print(f"Document ID: {doc['document_id']}")
                    print(f"Title: {doc['title']}")
                    print(f"URL: {doc['url']}")
                    print(f"Publication Date: {doc['publication_date']}")
                    print(f"Content preview: {doc['content'][:100]}...\n")
                
            elif choice == "3":
                confirm = input("Are you sure you want to clear all documents? (yes/no): ")
                if confirm.lower() == 'yes':
                    store.clear_collection()
                    print("Collection cleared successfully!")
                else:
                    print("Operation cancelled.")
                
            elif choice == "4":
                query = input("Enter search query: ")
                results = store.search_similar(query)
                print_separator()
                store.print_results(results)
                
            elif choice == "5":
                query = input("Enter search query: ")
                results = store.search_similar_bm25(query)
                print_separator()
                store.print_results(results)
                
            elif choice == "6":
                query = input("Enter search query: ")
                alpha = float(input("Enter alpha value (0.0 to 1.0, default 0.5): ") or "0.5")
                results = store.search_hybrid(query, alpha=alpha)
                print_separator()
                store.print_results(results)
                
            elif choice == "7":
                print("Exiting...")
                break
                
            else:
                print("Invalid choice. Please try again.")
                
            print_separator()
                
        except Exception as e:
            print(f"Error: {str(e)}")
            print("Please try again.")
            
        input("Press Enter to continue...")