import asyncio
import time
import os
from contextlib import asynccontextmanager
from typing import Dict, Any, Optional

from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
import uvicorn

from src.parsers.resume_processor import process_resume
from src.auth.auth_middleware import require_authentication, optional_authentication


# Load environment variables
load_dotenv()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan events for startup and shutdown"""

    # Startup
    print("🚀 Resume Parser API Starting...")
    print("📊 Production-ready for 100 concurrent users")

    # Verify environment variables
    required_vars = ['GEMINI_API_KEY', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'JWT_SECRET_KEY']
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        print(f"❌ Missing environment variables: {missing_vars}")
        raise ValueError(f"Missing required environment variables: {missing_vars}")

    print("✅ Environment variables validated")
    print("✅ Ready to process resumes")

    yield

    # Shutdown
    print("🛑 Resume Parser API Shutting down...")


# Initialize FastAPI with optimized settings for concurrency
app = FastAPI(
    title="Resume Parser API",
    description="Production-ready resume parsing service supporting PDF, DOC, DOCX, and Images",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware for frontend integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Global stats for monitoring
request_stats = {
    "total_requests": 0,
    "successful_requests": 0,
    "failed_requests": 0,
    "concurrent_requests": 0,
    "average_processing_time": 0.0
}


@app.post("/parse-resume")
async def parse_resume_endpoint(
    file: UploadFile = File(...),
    auth_user: Optional[Dict[str, Any]] = Depends(require_authentication)
) -> JSONResponse:
    """
    Single endpoint for resume parsing
    Supports: PDF, DOC, DOCX, PNG, JPG, JPEG
    Target: 15-second response time
    Concurrency: 100+ users
    """

    request_start = time.time()
    request_stats["total_requests"] += 1
    request_stats["concurrent_requests"] += 1

    try:
        # Validate file
        if not file.filename:
            raise HTTPException(status_code=400, detail="No file provided")

        # Check file size (10MB limit for Textract)
        content = await file.read()
        if len(content) > 10_000_000:  # 10MB
            raise HTTPException(status_code=413, detail="File too large (max 10MB)")

        # Validate file type
        allowed_extensions = ['pdf', 'doc', 'docx', 'png', 'jpg', 'jpeg', 'txt']
        file_ext = file.filename.lower().split('.')[-1] if '.' in file.filename else ''

        if file_ext not in allowed_extensions:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported file type: {file_ext}. Allowed: {', '.join(allowed_extensions)}"
            )

        # Log authenticated user
        user_id = auth_user.get("userId") if auth_user else "anonymous"
        print(f"📄 Processing: {file.filename} ({len(content)} bytes) for user: {user_id}")

        # Process the resume
        result = await process_resume(content, file.filename)

        # Add user context to response metadata
        if "metadata" in result:
            result["metadata"]["user_id"] = user_id

        # Update stats
        processing_time = time.time() - request_start
        request_stats["successful_requests"] += 1
        request_stats["average_processing_time"] = (
            (request_stats["average_processing_time"] * (request_stats["successful_requests"] - 1) + processing_time)
            / request_stats["successful_requests"]
        )

        print(f"✅ Completed: {file.filename} in {processing_time:.2f}s")

        return JSONResponse(
            status_code=200,
            content=result
        )

    except HTTPException:
        request_stats["failed_requests"] += 1
        raise

    except Exception as e:
        request_stats["failed_requests"] += 1
        processing_time = time.time() - request_start

        print(f"❌ Error processing {file.filename}: {str(e)}")

        # Return structured error response
        error_response = {
            "success": False,
            "error": {
                "message": str(e),
                "filename": file.filename,
                "processing_time_seconds": round(processing_time, 2),
                "timestamp": time.time()
            },
            "data": {
                "personalInfo": {
                    "name": "", "email": "", "phone": "", "location": "",
                    "linkedin": "", "github": "", "portfolio": ""
                },
                "experience": [],
                "education": [],
                "skills": {"technical": [], "languages": [], "certifications": []},
                "summary": ""
            }
        }

        return JSONResponse(
            status_code=500,
            content=error_response
        )

    finally:
        request_stats["concurrent_requests"] -= 1


@app.get("/health")
async def health_check() -> Dict[str, Any]:
    """
    Health check endpoint for monitoring
    """
    return {
        "status": "healthy",
        "timestamp": time.time(),
        "stats": request_stats,
        "environment": {
            "gemini_configured": bool(os.getenv('GEMINI_API_KEY')),
            "aws_configured": bool(os.getenv('AWS_ACCESS_KEY_ID')),
            "auth_configured": bool(os.getenv('JWT_SECRET_KEY')),
        }
    }


@app.get("/stats")
async def get_stats() -> Dict[str, Any]:
    """
    Detailed statistics endpoint
    """
    return {
        "request_stats": request_stats,
        "success_rate": (
            request_stats["successful_requests"] / request_stats["total_requests"] * 100
            if request_stats["total_requests"] > 0 else 0
        ),
        "average_processing_time": round(request_stats["average_processing_time"], 2),
        "concurrent_requests": request_stats["concurrent_requests"]
    }


@app.post("/parse-resume-test")
async def parse_resume_test_endpoint(file: UploadFile = File(...)) -> JSONResponse:
    """
    Test endpoint for resume parsing without authentication
    Use this for testing and development only
    """
    return await parse_resume_endpoint(file, auth_user=None)


@app.get("/")
async def root():
    """
    API root endpoint
    """
    return {
        "message": "Resume Parser API",
        "version": "1.0.0",
        "status": "running",
        "authentication": {
            "required": True,
            "type": "JWT Bearer Token",
            "header": "Authorization: Bearer <token>"
        },
        "endpoints": {
            "parse": "/parse-resume (requires auth)",
            "parse_test": "/parse-resume-test (no auth, for testing)",
            "health": "/health",
            "stats": "/stats"
        },
        "supported_formats": ["PDF", "DOC", "DOCX", "PNG", "JPG", "JPEG"],
        "max_file_size": "10MB",
        "target_response_time": "15 seconds"
    }


# Concurrency settings for production
if __name__ == "__main__":
    print("🚀 Starting Resume Parser API...")
    print("💡 For production deployment, use: uvicorn app:app --host 0.0.0.0 --port 8000 --workers 4")

    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8000,
        reload=False,  # Set to False for production
        access_log=True,
        log_level="info",
        workers=1  # For development; use multiple workers in production
    )