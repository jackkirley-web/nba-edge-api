# main.py -- SportEdge API
# NBA:        /api/*
# AFL:        /api/afl/*
# Greyhounds: /api/grey/*

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes import router as nba_router
from routes_afl import router as afl_router
from routes_greyhound import router as grey_router

app = FastAPI(title="SportEdge API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(nba_router)
app.include_router(afl_router)
app.include_router(grey_router)

@app.get("/")
def root():
    return {"status": "ok", "service": "SportEdge API", "sports": ["NBA", "AFL", "Greyhounds"]}

@app.get("/health")
def health():
    return {"status": "ok"}
