import os
import io
import fitz  # PyMuPDF
import pdfplumber
import docx
from PIL import Image
import json
import hashlib
import pickle
from functools import lru_cache
from typing import Optional, Any
from dotenv import load_dotenv

load_dotenv()

from openai import OpenAI
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Initialize Redis client (optional - graceful fallback if not available)
redis_client = None
try:
    import redis
    redis_client = redis.Redis(
        host=os.getenv('REDIS_HOST', 'localhost'),
        port=int(os.getenv('REDIS_PORT', 6379)),
        db=0,
        decode_responses=False,
        socket_connect_timeout=1,
        socket_timeout=1
    )
    redis_client.ping()  # Test connection
except Exception:
    # Redis not available or not installed - use only in-memory cache
    redis_client = None

# In-memory cache as fallback
_cache = {}
_max_cache_size = 1000
_cache_ttl = 86400 * 7  # 7 days

def _get_file_hash(content: bytes) -> str:
    """Generate hash of file content for cache keys"""
    return hashlib.md5(content).hexdigest()

def _get_text_hash(text: str) -> str:
    """Generate hash of text content for cache keys"""
    return hashlib.md5(text.encode('utf-8')).hexdigest()

def _get_from_cache(key: str) -> Optional[Any]:
    """Get item from cache (Redis first, then in-memory)"""
    try:
        if redis_client:
            cached = redis_client.get(key)
            if cached:
                return pickle.loads(cached)
    except Exception:
        pass
    
    return _cache.get(key)

def _set_cache(key: str, value: Any):
    """Set item in cache (both Redis and in-memory)"""
    try:
        if redis_client:
            redis_client.setex(key, _cache_ttl, pickle.dumps(value))
    except Exception:
        pass
    
    # Also store in memory with size limit
    if len(_cache) >= _max_cache_size:
        # Remove oldest 25% of items
        keys_to_remove = list(_cache.keys())[:_max_cache_size // 4]
        for k in keys_to_remove:
            del _cache[k]
    
    _cache[key] = value

# Cache the expensive OCR reader initialization
@lru_cache(maxsize=1)
def _get_ocr_reader():
    import easyocr
    return easyocr.Reader(['en'], gpu=False)

# Get cached reader instance
reader = _get_ocr_reader()

def extract_text_from_resume(filename: str, content: bytes) -> str:
    """Extract text with caching - same interface as original"""
    # Check cache first
    file_hash = _get_file_hash(content)
    cache_key = f"text_extract:{file_hash}"
    
    cached_text = _get_from_cache(cache_key)
    if cached_text is not None:
        return cached_text

    ext = os.path.splitext(filename)[1].lower()

    if ext == ".pdf":
        text = extract_text_from_pdf(content)
    elif ext == ".docx":
        text = extract_text_from_docx(content)
    elif ext in [".png", ".jpg", ".jpeg"]:
        text = extract_text_from_image(content)
    else:
        return "Unsupported file format."
    
    # Cache the result
    _set_cache(cache_key, text)
    return text

def extract_text_from_pdf(content: bytes) -> str:
    """PDF text extraction with OCR fallback and caching"""
    file_hash = _get_file_hash(content)
    
    text = ""
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        for page in pdf.pages:
            text += page.extract_text() or ""

    if not text.strip():
        # Check OCR cache
        ocr_cache_key = f"pdf_ocr:{file_hash}"
        cached_ocr = _get_from_cache(ocr_cache_key)
        if cached_ocr is not None:
            return cached_ocr
        
        # Perform OCR and cache result
        text = extract_text_from_pdf_with_ocr(content)
        _set_cache(ocr_cache_key, text)

    return text

def extract_text_from_pdf_with_ocr(content: bytes) -> str:
    """OCR extraction for PDFs - same as original but using cached reader"""
    doc = fitz.open(stream=content, filetype="pdf")
    text = ""
    for page in doc:
        pix = page.get_pixmap()
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        import numpy as np
        img_np = np.array(img)
        result = reader.readtext(img_np, detail=0)
        text += "\n".join(result) + "\n"
    return text

def extract_text_from_docx(content: bytes) -> str:
    """DOCX text extraction - same as original"""
    doc = docx.Document(io.BytesIO(content))
    return "\n".join([para.text for para in doc.paragraphs])

def extract_text_from_image(content: bytes) -> str:
    """Image OCR with caching - same interface as original"""
    file_hash = _get_file_hash(content)
    cache_key = f"image_ocr:{file_hash}"
    
    # Check cache first
    cached_text = _get_from_cache(cache_key)
    if cached_text is not None:
        return cached_text
    
    # Perform OCR using cached reader
    image = Image.open(io.BytesIO(content)).convert("RGB")
    import numpy as np
    img_np = np.array(image)
    result = reader.readtext(img_np, detail=0)
    text = "\n".join(result)
    
    # Cache the result
    _set_cache(cache_key, text)
    return text

def transform_text_to_resume_data(raw_text: str) -> dict:
    """Transform text to structured data with OpenAI API caching - same interface"""
    # Check cache first
    text_hash = _get_text_hash(raw_text)
    cache_key = f"openai_transform:{text_hash}"
    
    cached_result = _get_from_cache(cache_key)
    if cached_result is not None:
        return cached_result

    prompt = f"""
You are a resume parser. Extract structured resume data in the following JSON format.

Expected JSON:
{{
  "id": null,
  "targetJobTitle": "",
  "targetJobDescription": "",
  "personalInfo": {{
    "fullName": "",
    "jobTitle": "",
    "email": "",
    "phone": "",
    "location": "",
    "summary": "",
    "profilePicture": null
  }},
  "sections": [
    {{
      "id": "null",
      "type": "experience",
      "title": "Work Experience",
      "order": 0,
      "hidden": false,
      "items": [
        {{
          "jobTitle": "",
          "company": "",
          "location": "",
          "startDate": null,
          "endDate": null,
          "currentPosition": false,
          "description": ""
        }}
      ],
      "groups": [],
      "state": {{}}
    }},
    {{
      "id": "null",
      "type": "projects",
      "title": "Projects",
      "order": 1,
      "hidden": false,
      "items": [],
      "groups": [],
      "state": {{}}
    }},
    {{
      "id": "null",
      "type": "education",
      "title": "Education",
      "order": 2,
      "hidden": false,
      "items": [],
      "groups": [],
      "state": {{}}
    }},
    {{
      "id": "null",
      "type": "skills",
      "title": "Skills",
      "order": 3,
      "format": "grouped",
      "items": [],
      "groups": [],
      "state": {{
        "categoryOrder": [],
        "viewMode": "categorized"
      }},
      "hidden": false
    }}
  ]
}}

Resume Text:
\"\"\"
{raw_text}
\"\"\"

Return only valid JSON.
"""

    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2
        )

        content = response.choices[0].message.content.strip()
        result = json.loads(content)
        
        # Cache successful result
        _set_cache(cache_key, result)
        return result
        
    except Exception as e:
        error_result = {"error": str(e)}
        # Don't cache errors
        return error_result

def parse_resume(filename: str, content: bytes) -> dict:
    """Main parsing function with full pipeline caching - same interface as original"""
    # Check for complete cached result first
    file_hash = _get_file_hash(content)
    cache_key = f"full_parse:{file_hash}"
    
    cached_result = _get_from_cache(cache_key)
    if cached_result is not None:
        return cached_result
    
    # Process normally with individual step caching
    raw_text = extract_text_from_resume(filename, content)
    structured_data = transform_text_to_resume_data(raw_text)
    
    # Cache the complete result
    _set_cache(cache_key, structured_data)
    return structured_data

# Optional: Cache management functions for monitoring
def get_cache_stats():
    """Get cache statistics for monitoring"""
    stats = {
        "in_memory_size": len(_cache),
        "redis_available": redis_client is not None
    }
    
    if redis_client:
        try:
            info = redis_client.info()
            stats["redis_used_memory"] = info.get("used_memory_human", "N/A")
            stats["redis_keys"] = redis_client.dbsize()
        except Exception:
            stats["redis_error"] = "Could not get Redis stats"
    
    return stats

def clear_cache():
    """Clear all caches"""
    global _cache
    _cache.clear()
    
    if redis_client:
        try:
            redis_client.flushdb()
        except Exception:
            pass