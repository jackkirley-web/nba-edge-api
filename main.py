# main.py -- SportEdge API
# NBA routes: /api/*
# AFL routes: /api/afl/*

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes import router as nba_router
from routes_afl import router as afl_router

app = FastAPI(title="SportEdge API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(nba_router)
app.include_router(afl_router)

@app.get("/")
def root():
    return {"status": "ok", "service": "SportEdge API", "sports": ["NBA", "AFL"]}

@app.get("/health")
def health():
    return {"status": "ok"}
