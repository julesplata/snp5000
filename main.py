from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from database import engine, Base
import uvicorn

import app.models  # noqa: F401 ensures models are registered
from app.api import stocks, ratings, sectors, macro, news, quotes

# Create database tables
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="Stock Rating API",
    description="API for stock ratings combining multiple data sources",
    version="1.0.0",
)

# CORS middleware for frontend local
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# routers
app.include_router(stocks.router, prefix="/api/stocks", tags=["stocks"])
app.include_router(ratings.router, prefix="/api/ratings", tags=["ratings"])
app.include_router(sectors.router, prefix="/api/sectors", tags=["sectors"])
app.include_router(macro.router, prefix="/api/macro", tags=["macroeconomics"])
app.include_router(news.router, prefix="/api", tags=["news"])
app.include_router(quotes.router, prefix="/api", tags=["quotes"])


@app.get("/health")
def health_check():
    return {"status": "healthy"}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
