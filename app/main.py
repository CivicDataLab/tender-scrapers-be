from fastapi import FastAPI

from app.routers import dataset_router

app=FastAPI(title="CKAN Integration API")

app.include_router(dataset_router.router, prefix="/api/v1",tags=["datasets"])

@app.get("/health-check")
def read_root():
    return {
       "Status":"OK",
       "Message":"All Systems Narmal"
    }
