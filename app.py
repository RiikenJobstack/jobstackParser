from fastapi import FastAPI, UploadFile, File, Request, HTTPException, Form
from typing import Optional
from dotenv import load_dotenv
from token_service import verify_token
from user_service import find_user_by_id
import os
from fastapi.middleware.cors import CORSMiddleware
from utils import parse_resume
from utils import parse_resume_temp
from utils import parse_resume_cached, init_prompt_cache
import time

# Load environment variables (for OPENAI_API_KEY)
load_dotenv()
resume_cache_id = None
resume_cache_expiry = None
app = FastAPI()
# CORS configuration

def get_cors_origins():
    NODE_ENV = os.getenv("NODE_ENV", "development")
    print(f"NODE_ENV: {NODE_ENV}")

    if NODE_ENV == "production":
        return [
            'https://app.jobstack.ai',
            'http://localhost:5173',
            'https://jobstack.azurewebsites.net',
            'https://jobtackui-fgcdftezgkhbbpbg.canadacentral-01.azurewebsites.net'
        ]
    elif NODE_ENV == "uat":
        return [
            'http://localhost:5173',
            'https://jobstackuiuat-cybnbdf8h6gkb7g3.canadacentral-01.azurewebsites.net',
            'https://app-uat.jobstack.ai'
        ]
    else:  # default to development
        return [
            'http://localhost:3000',
            'http://localhost:5173',
            'https://app.jobstack.ai',
            'https://app-uat.jobstack.ai',
            'https://jobtackui-fgcdftezgkhbbpbg.canadacentral-01.azurewebsites.net',
            'https://jobstackuiuat-cybnbdf8h6gkb7g3.canadacentral-01.azurewebsites.net',
            'https://jobstackuidev-gwakgfdgbgh5emdw.canadacentral-01.azurewebsites.net'
        ]


app.add_middleware(
    CORSMiddleware,
    allow_origins=get_cors_origins(),  # only allow these domains
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

    if not origin or origin not in get_cors_origins():
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


@app.post("/temp/parse-resume")
async def upload_resume(
    request: Request,
    fileType: str = Form(...),          # either "file" or "text"
    file: Optional[UploadFile] = File(None),  
    text: Optional[str] = Form(None)  
):
    # Check origin
    origin = request.headers.get("origin")
    print(f"Origin: {origin}")

    if not origin or origin not in get_cors_origins():
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

    # Process resume based on fileType
    parsed_data = None
    if fileType == "file":
        if not file:
            raise HTTPException(status_code=400, detail="File not provided")
        content = await file.read()
        parsed_data = parse_resume_temp(file.filename, content)['structured_data']

    elif fileType == "text":
        if not text or not text.strip():
            raise HTTPException(status_code=400, detail="Text not provided")
        parsed_data = parse_resume_temp("resume.txt", text.encode("utf-8"))['structured_data']

    else:
        raise HTTPException(status_code=400, detail="Invalid fileType. Must be 'file' or 'text'.")

    return {
        "resumeData": parsed_data,
        "debug":parse_resume_temp(file.filename, content)['debug'],
        "userId": user.get("_id") if isinstance(user, dict) else user.id
    }

@app.post("/cached/parse-resume")
async def upload_resume_cached(
    request: Request,
    fileType: str = Form(...),          # either "file" or "text"
    file: Optional[UploadFile] = File(None),  
    text: Optional[str] = Form(None)  
):
    """Enhanced resume parsing endpoint with Gemini prompt caching for better performance"""
    
    # Check origin
    origin = request.headers.get("origin")
    print(f"Origin: {origin}")

    if not origin or origin not in get_cors_origins():
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

    # Process resume based on fileType using cached parsing
    start_time = time.time()
    
    try:
        if fileType == "file":
            if not file:
                raise HTTPException(status_code=400, detail="File not provided")
            content = await file.read()
            parsed_result = parse_resume_cached(file.filename, content)

        elif fileType == "text":
            if not text or not text.strip():
                raise HTTPException(status_code=400, detail="Text not provided")
            parsed_result = parse_resume_cached("resume.txt", text.encode("utf-8"))

        else:
            raise HTTPException(status_code=400, detail="Invalid fileType. Must be 'file' or 'text'.")

        processing_time = time.time() - start_time
        
        # Add performance metrics to response
        performance_info = {
            "processing_time": round(processing_time, 3),
            "cache_time_left_minutes": 0
        }
        
        if resume_cache_expiry:
            now = time.time() * 1000
            performance_info["cache_time_left_minutes"] = max(round((resume_cache_expiry - now) / 60000), 0)

        return {
            "success": parsed_result.get("success", False),
            "resumeData": parsed_result.get("data"),
            "error": parsed_result.get("error"),
            "debug": parsed_result.get("debug", {}),
            "userId": user.get("_id") if isinstance(user, dict) else user.id,
            "performance": performance_info
        }

    except Exception as e:
        processing_time = time.time() - start_time
        print(f"Error processing resume: {str(e)}")
        
        # Return error with fallback to original parsing method
        try:
            print("Attempting fallback to non-cached parsing...")
            if fileType == "file":
                content = await file.read()
                parsed_data = parse_resume_temp(file.filename, content)
            elif fileType == "text":
                parsed_data = parse_resume_temp("resume.txt", text.encode("utf-8"))
                
            return {
                "success": True,
                "resumeData": parsed_data,
                "userId": user.get("_id") if isinstance(user, dict) else user.id,
                "performance": {
                    "processing_time": round(time.time() - start_time, 3),
                    "cache_used": False,
                    "fallback_used": True,
                    "error": "Cached parsing failed, used fallback method"
                }
            }
        except Exception as fallback_error:
            raise HTTPException(
                status_code=500, 
                detail={
                    "error": "Resume parsing failed", 
                    "original_error": str(e),
                    "fallback_error": str(fallback_error),
                    "processing_time": round(time.time() - start_time, 3)
                }
            )

# Optional: Cache management endpoint for monitoring
@app.get("/cached/cache-status")
async def get_cache_status():
    """Get current cache status and statistics"""
    global resume_cache_id, resume_cache_expiry
    
    now = time.time() * 1000
    cache_active = resume_cache_id is not None and resume_cache_expiry is not None and now < resume_cache_expiry
    
    status = {
        "cache_active": cache_active,
        "cache_id": resume_cache_id,
        "time_left_minutes": 0 if not cache_active else max(round((resume_cache_expiry - now) / 60000), 0)
    }
    
    return status

# Optional: Manual cache refresh endpoint
@app.post("/cached/refresh-cache")
async def refresh_cache():
    """Manually refresh the prompt cache"""
    global resume_cache_id, resume_cache_expiry
    
    try:
        # Force cache refresh
        resume_cache_id = None
        resume_cache_expiry = None
        
        # Initialize new cache
        cache_id = init_prompt_cache("gemini-2.5-flash-lite")
        
        return {
            "success": True,
            "message": "Cache refreshed successfully",
            "cache_id": cache_id,
            "expires_in_hours": 23
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Failed to refresh cache",
                "details": str(e)
            }
        )