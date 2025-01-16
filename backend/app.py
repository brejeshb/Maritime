from fastapi import FastAPI
from routes import router  # we'll modify routes.py to use APIRouter

app = FastAPI()
app.include_router(router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

    # can run  uvicorn app:app --reload in console
    # or python3 app.py?
