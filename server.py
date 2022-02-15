from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
import boto3

S3_BUCKET = "dewi-etl-data-dumps"
URL_PREFIX = f"https://{S3_BUCKET}.s3.amazonaws.com/"

app = FastAPI()
client = boto3.client("s3")
folders = [f["Prefix"] for f in client.list_objects(Bucket=S3_BUCKET, Delimiter="/")["CommonPrefixes"]]
objects_cache = {}
for folder in folders:
    files = [f for f in client.list_objects(Bucket=S3_BUCKET, Prefix=folder)["Contents"]]
    objects_cache[folder[:-1]] = files

templates = Jinja2Templates("templates")


@app.get("/")
async def homepage(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "folders": folders})


@app.get("/{folder}")
async def files_in_folder(request: Request, folder: str):
    return templates.TemplateResponse("folder.html", {"request": request, "folder": folder, "url_prefix": URL_PREFIX, "files": objects_cache[folder]})




