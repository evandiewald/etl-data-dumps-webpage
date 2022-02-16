from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
import boto3
from cachetools import cached, TTLCache


S3_BUCKET = "dewi-etl-data-dumps"
URL_PREFIX = f"https://{S3_BUCKET}.s3.amazonaws.com/"

app = FastAPI()
client = boto3.client("s3")
templates = Jinja2Templates("templates")


@cached(TTLCache(maxsize=1, ttl=600))
def get_objects():
    folders = [f["Prefix"] for f in client.list_objects(Bucket=S3_BUCKET, Delimiter="/")["CommonPrefixes"]]
    folders_cache = {}
    for folder in folders:
        files = [f for f in client.list_objects(Bucket=S3_BUCKET, Prefix=folder)["Contents"]]
        folders_cache[folder[:-1]] = files
    return folders, folders_cache


@app.get("/")
async def homepage(request: Request):
    folders, folders_cache = get_objects()
    return templates.TemplateResponse("index.html", {"request": request, "folders": folders})


@app.get("/folder/{folder}/")
async def files_in_folder(request: Request, folder: str):
    folders, folders_cache = get_objects()
    files = folders_cache[folder]
    return templates.TemplateResponse("folder.html", {"request": request, "folder": folder, "url_prefix": URL_PREFIX, "files": files})



