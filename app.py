from fastapi import FastAPI, UploadFile, File, Request, HTTPException
from dotenv import load_dotenv
from token_service import verify_token
from user_service import find_user_by_id
import uvicorn
from fastapi.middleware.cors import CORSMiddleware
from resume_parser import parse_resume

# Load environment variables (for OPENAI_API_KEY)
load_dotenv()

app = FastAPI()

origins = [
    "https://jobstackuidev-gwakgfdgbgh5emdw.canadacentral-01.azurewebsites.net",
    "https://jobtackui-fgcdftezgkhbbpbg.canadacentral-01.azurewebsites.net",
    "http://localhost:5173",
    "https://jobtackui-fgcdftezgkhbbpbg.canadacentral-01.azurewebsites.net"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # only allow these domains
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def health_check():
    return {"status": "ok", "message": "Resume parser is running"}


@app.post("/parse-resume")
async def upload_resume(request: Request, file: UploadFile = File(...)):
    # Check origin
    origin = request.headers.get("origin")
    print(f"Origin: {origin}")

    if not origin or origin not in origins:
        raise HTTPException(status_code=403, detail="Origin not allowed")

    # Check Authorization header
    auth_header = request.headers.get("authorization")
    if not auth_header:
        raise HTTPException(
            status_code=400,
            detail={"error": "Authorization header is missing", "status": 400}
        )

    # Extract Bearer token
    try:
        token = auth_header.split(" ")[1]
    except IndexError:
        raise HTTPException(
            status_code=400,
            detail={"error": "Bearer token is missing", "status": 400}
        )
    
    if not token:
        raise HTTPException(
            status_code=400,
            detail={"error": "Bearer token is missing", "status": 400}
        )

    # Verify token
    try:
        decoded_token = await verify_token(token)
    except Exception as err:
        raise HTTPException(
            status_code=401,
            detail={
                "error": "Invalid or expired token",
                "status": 401,
                "details": str(err)
            }
        )

    # Extract userId from token
    user_id = decoded_token.get("userId")
    if not user_id:
        raise HTTPException(
            status_code=400,
            detail={"error": "Invalid token payload", "status": 400}
        )

    # Find user in database
    user = await find_user_by_id(user_id)
    if not user:
        raise HTTPException(
            status_code=401,
            detail={"error": "User does not exist", "status": 401}
        )

    # Process the resume if all checks pass
    content = await file.read()
    parsed_data = parse_resume(file.filename, content)
    
    return {
        "resumeData": parsed_data,
        "userId": user.get("_id") if isinstance(user, dict) else user.id
    }