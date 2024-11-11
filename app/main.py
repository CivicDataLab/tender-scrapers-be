from typing import Union
from fastapi import FastAPI

app=FastAPI()
@app.get("/")
def read_root():
    return {
        "Hello":"World"
    }

@app.get("/items/{id}")
def read_item(id:int, q:Union[str,None]=None):
    return {"id":id, "q":q}