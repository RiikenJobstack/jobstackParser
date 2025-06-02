from fastapi import FastAPI, UploadFile, File
from dotenv import load_dotenv
import uvicorn
from fastapi.middleware.cors import CORSMiddleware
from resume_parser import parse_resume

# Load environment variables (for OPENAI_API_KEY)
load_dotenv()

app = FastAPI()

# CORS setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Set to specific domain in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def health_check():
    return {"status": "ok", "message": "Resume parser is running"}


@app.post("/parse-resume")
async def upload_resume(file: UploadFile = File(...)):
    content = await file.read()
    parsed_data = parse_resume(file.filename, content)
    return {"resumeData": parsed_data}