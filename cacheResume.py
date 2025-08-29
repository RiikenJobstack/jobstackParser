import time
import threading
import json
import re
from typing import Optional, Dict, Any
import requests
import google.auth
import google.auth.transport.requests
from credentials import GOOGLE_APPLICATION_CREDENTIALS


# Import static prompts
from static_prompt import STATIC_RESUME_PARSER_PROMPT
from dotenv import load_dotenv

# Cache utility functions - add these to your file

import hashlib
import pickle
from typing import Optional, Any
import os

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

load_dotenv()

# Initialize Google Generative AI client
import google.generativeai as genai
from google.oauth2 import service_account
import google.auth.transport.requests
import requests

# Set up credentials with proper scopes
credentials = service_account.Credentials.from_service_account_info(
    GOOGLE_APPLICATION_CREDENTIALS,
    scopes=["https://www.googleapis.com/auth/cloud-platform"]
)

# Global cache variables for prompt caching
resume_cache_id = None
resume_cache_expiry = None
resume_cache_lock = threading.Lock()

def create_prompt_cache(static_instructions: str, model: str) -> str:
    """Create cache entry for static instructions"""
    try:
        # Get access token
        auth_req = google.auth.transport.requests.Request()
        credentials.refresh(auth_req)
        access_token = credentials.token

        endpoint = f"https://us-central1-aiplatform.googleapis.com/v1/projects/{GOOGLE_APPLICATION_CREDENTIALS['project_id']}/locations/us-central1/cachedContents"

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        data = {
            "model": f"projects/{GOOGLE_APPLICATION_CREDENTIALS['project_id']}/locations/us-central1/publishers/google/models/{model}",
            "contents": [
                {
                    "role": "user",
                    "parts": [{"text": static_instructions}]
                }
            ],
            "ttl": "86400s"  # 24 hours
        }

        response = requests.post(endpoint, headers=headers, json=data, timeout=30)
        response.raise_for_status()

        result = response.json()
        return result["name"]

    except Exception as e:
        print(f"Cache creation failed: {str(e)}")
        raise e

def init_prompt_cache(model: str) -> str:
    """Initialize and manage prompt cache with thread safety"""
    global resume_cache_id, resume_cache_expiry

    now = time.time() * 1000  # Convert to milliseconds

    if resume_cache_id and resume_cache_expiry and now < resume_cache_expiry:
        return resume_cache_id
        
    with resume_cache_lock:
        # Double-check after acquiring lock
        if resume_cache_id and resume_cache_expiry and now < resume_cache_expiry:
            return resume_cache_id
        
        print("Creating resume parser prompt cache...")
        resume_cache_id = create_prompt_cache(STATIC_RESUME_PARSER_PROMPT, model)
        resume_cache_expiry = time.time() * 1000 + (23 * 60 * 60 * 1000)  # 23 hours
        print(f"Resume Parser Cache created: {resume_cache_id}")
        return resume_cache_id

def build_dynamic_prompt(resume_text: str) -> str:
    """Build dynamic prompt that references cached instructions"""
    return f"""Use the cached instructions.

<<<RESUME_START>>>
{resume_text}
<<<RESUME_END>>>

RESPONSE FORMAT:
Return ONLY the JSON object as specified in the cached schema. No markdown, no explanations."""

def count_tokens(prompt: str, model: str) -> int:
    """Count tokens in the prompt"""
    try:
        # Get access token
        auth_req = google.auth.transport.requests.Request()
        credentials.refresh(auth_req)
        access_token = credentials.token

        endpoint = f"https://us-central1-aiplatform.googleapis.com/v1/projects/{GOOGLE_APPLICATION_CREDENTIALS['project_id']}/locations/us-central1/publishers/google/models/{model}:countTokens"

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        data = {
            "contents": [
                {
                    "role": "USER",
                    "parts": [{"text": prompt}]
                }
            ]
        }

        response = requests.post(endpoint, headers=headers, json=data, timeout=30)
        response.raise_for_status()

        result = response.json()
        return result.get("totalTokens", 0)

    except Exception as e:
        print(f"Token counting failed: {str(e)}")
        return 0

def calculate_total_cost(input_tokens: int, output_tokens: int) -> float:
    """Calculate total cost based on token usage"""
    input_cost = input_tokens * 0.25 / 1_000_000
    output_cost = output_tokens * 0.75 / 1_000_000
    return input_cost + output_cost

def call_gemini_with_cache_and_retry(dynamic_prompt: str, model: str, max_retries: int = 2) -> str:
    """Call Gemini API with caching and retry logic"""
    global resume_cache_id, resume_cache_expiry
    
    retries = 0

    while retries < max_retries:
        try:
            cache_id = init_prompt_cache(model)
            
            # Get access token
            auth_req = google.auth.transport.requests.Request()
            credentials.refresh(auth_req)
            access_token = credentials.token

            endpoint = f"https://us-central1-aiplatform.googleapis.com/v1/projects/{GOOGLE_APPLICATION_CREDENTIALS['project_id']}/locations/us-central1/publishers/google/models/{model}:generateContent"

            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json"
            }

            data = {
                "cachedContent": cache_id,
                "contents": [
                    {
                        "role": "user",
                        "parts": [{"text": dynamic_prompt}]
                    }
                ],
                "generationConfig": {
                    "maxOutputTokens": 8000,
                    "temperature": 0.1,
                    "topP": 0.8,
                    "topK": 40
                }
            }

            response = requests.post(endpoint, headers=headers, json=data, timeout=30)
            response.raise_for_status()

            result = response.json()

            # Extract text from response
            if (result.get("candidates") and
                result["candidates"][0].get("content") and
                result["candidates"][0]["content"].get("parts") and
                result["candidates"][0]["content"]["parts"][0].get("text")):

                return result["candidates"][0]["content"]["parts"][0]["text"]
            else:
                raise Exception("Invalid response structure from Gemini API")

        except Exception as err:
            if "Cache content" in str(err) and retries < max_retries:
                print(f"Gemini cache expired, retrying attempt {retries + 1}")
                # Force refresh local cache
                resume_cache_id = None
                resume_cache_expiry = None
                retries += 1
                continue
            raise err

# def parse_gemini_response(content: str) -> Dict[str, Any]:
#     """Parse Gemini response with multiple fallback strategies"""
#     if not content or not isinstance(content, str):
#         raise ValueError("Invalid content provided")

#     print(f"Parsing content (length: {len(content)})")

#     # Strategy 1: Try direct JSON parse
#     try:
#         result = json.loads(content.strip())
#         print("Strategy 1: Direct parse successful")
#         return result
#     except json.JSONDecodeError as e:
#         print(f"Strategy 1 failed: {str(e)}")

#     # Strategy 2: Extract from code blocks  
#     print("Strategy 2: Trying code block extraction...")
#     code_block_patterns = [
#         r'```json\s*([\s\S]*?)\s*```',
#         r'```\s*([\s\S]*?)\s*```',
#         r'`\s*(\{[\s\S]*?\})\s*`'
#     ]

#     for i, pattern in enumerate(code_block_patterns):
#         matches = re.findall(pattern, content, re.IGNORECASE | re.DOTALL)
#         if matches:
#             print(f"Found {len(matches)} matches for pattern {i+1}")
#             for j, match in enumerate(matches):
#                 try:
#                     cleaned = match.strip()
#                     result = json.loads(cleaned)
#                     print("Strategy 2: Code block extraction successful")
#                     return result
#                 except json.JSONDecodeError:
#                     continue

#     # Strategy 3: Find JSON-like content
#     print("Strategy 3: Trying JSON pattern extraction...")
#     json_pattern = r'\{[\s\S]*\}'
#     json_match = re.search(json_pattern, content, re.DOTALL)
#     if json_match:
#         try:
#             json_content = json_match.group(0)
#             result = json.loads(json_content)
#             print("Strategy 3: JSON pattern extraction successful")
#             return result
#         except json.JSONDecodeError as e:
#             print(f"Strategy 3 failed: {str(e)}")

#     # Strategy 4: Try to fix common JSON issues
#     print("Strategy 4: Trying to fix common JSON issues...")
#     try:
#         fixed_content = content
#         # Remove trailing commas
#         fixed_content = re.sub(r',(\s*[}\]])', r'\1', fixed_content)
#         # Quote unquoted keys
#         fixed_content = re.sub(r'([{,]\s*)(\w+):', r'\1"\2":', fixed_content)
#         # Replace single quotes with double quotes
#         fixed_content = re.sub(r":\s*'([^']*)'", r': "\1"', fixed_content)
#         fixed_content = fixed_content.strip()
        
#         result = json.loads(fixed_content)
#         print("Strategy 4: JSON fixing successful")
#         return result
#     except json.JSONDecodeError as e:
#         print(f"Strategy 4 failed: {str(e)}")

#     # If all strategies fail, throw an error
#     content_preview = content[:500] if len(content) > 500 else content
#     print("All strategies failed")
#     raise ValueError(f"Failed to parse JSON after all strategies. Content preview: {content_preview}...")

def parse_gemini_response(content: str) -> Dict[str, Any]:
    """
    Parse Gemini response with improved regex patterns and error handling
    """
    if not content or not isinstance(content, str):
        raise ValueError("Invalid content provided")

    print(f"Parsing content (length: {len(content)})")
    
    # Strategy 1: Try direct JSON parse
    try:
        result = json.loads(content.strip())
        print("✅ Strategy 1: Direct parse successful")
        return result
    except json.JSONDecodeError as e:
        print(f"❌ Strategy 1 failed: {str(e)}")

    # Strategy 2: Extract from code blocks with improved patterns
    print("🔄 Strategy 2: Trying code block extraction...")
    
    # More comprehensive code block patterns
    code_block_patterns = [
        # Standard markdown code blocks
        r'```json\s*\n?(.*?)\n?```',
        r'```\s*\n?(.*?)\n?```',
        # Code blocks without newlines
        r'```json\s*(.*?)\s*```',
        r'```\s*(.*?)\s*```',
        # Single backticks with JSON
        r'`\s*(\{.*?\})\s*`',
        # JSON wrapped in any amount of backticks
        r'`+\s*(\{[\s\S]*?\})\s*`+',
    ]

    for i, pattern in enumerate(code_block_patterns):
        print(f"   Trying pattern {i+1}: {pattern}")
        try:
            # Use DOTALL flag to match newlines in the content
            matches = re.findall(pattern, content, re.IGNORECASE | re.DOTALL)
            if matches:
                print(f"   Found {len(matches)} matches")
                for j, match in enumerate(matches):
                    try:
                        print(f"   Processing match {j+1} (length: {len(match)})")
                        # Clean the match more thoroughly
                        cleaned = match.strip()
                        
                        # Remove any remaining backticks or markdown artifacts
                        cleaned = re.sub(r'^`+|`+$', '', cleaned)
                        cleaned = re.sub(r'^json\s*', '', cleaned, flags=re.IGNORECASE)
                        cleaned = cleaned.strip()
                        
                        print(f"   Cleaned match preview: {cleaned[:200]}...")
                        result = json.loads(cleaned)
                        print("✅ Strategy 2: Code block extraction successful")
                        return result
                    except json.JSONDecodeError as e:
                        print(f"   Match {j+1} failed: {str(e)}")
                        continue
            else:
                print(f"   No matches found for pattern {i+1}")
        except Exception as e:
            print(f"   Pattern {i+1} error: {str(e)}")
            continue

    # Strategy 3: Find JSON-like content with better regex
    print("🔄 Strategy 3: Trying JSON pattern extraction...")
    try:
        # Look for JSON object that starts with { and ends with }
        # This pattern handles nested braces correctly
        json_pattern = r'\{(?:[^{}]|(?:\{[^{}]*\})*)*\}'
        matches = re.findall(json_pattern, content, re.DOTALL)
        
        if matches:
            # Try the longest match first (most likely to be complete)
            matches.sort(key=len, reverse=True)
            for i, match in enumerate(matches[:3]):  # Try top 3 matches
                try:
                    print(f"   Trying JSON match {i+1} (length: {len(match)})")
                    result = json.loads(match)
                    print("✅ Strategy 3: JSON pattern extraction successful")
                    return result
                except json.JSONDecodeError as e:
                    print(f"   JSON match {i+1} failed: {str(e)}")
                    continue
        else:
            print("   No JSON patterns found")
    except Exception as e:
        print(f"   Strategy 3 error: {str(e)}")

    # Strategy 4: Advanced JSON cleaning and fixing
    print("🔄 Strategy 4: Advanced JSON fixing...")
    try:
        # Start with the original content
        fixed_content = content
        
        # Remove markdown code block markers
        fixed_content = re.sub(r'```(?:json)?\s*\n?', '', fixed_content, flags=re.IGNORECASE)
        fixed_content = re.sub(r'\n?```\s*$', '', fixed_content)
        
        # Remove any leading/trailing backticks
        fixed_content = re.sub(r'^`+|`+$', '', fixed_content.strip())
        
        # Remove "json" keyword if it appears at the start
        fixed_content = re.sub(r'^\s*json\s*', '', fixed_content, flags=re.IGNORECASE)
        
        # Fix common JSON issues
        fixed_content = re.sub(r',(\s*[}\]])', r'\1', fixed_content)  # Remove trailing commas
        fixed_content = re.sub(r'([{,]\s*)(\w+):', r'\1"\2":', fixed_content)  # Quote unquoted keys
        fixed_content = re.sub(r":\s*'([^']*)'", r': "\1"', fixed_content)  # Replace single quotes
        
        # Clean up whitespace
        fixed_content = fixed_content.strip()
        
        print(f"   Fixed content preview: {fixed_content[:200]}...")
        result = json.loads(fixed_content)
        print("✅ Strategy 4: Advanced JSON fixing successful")
        return result
    except json.JSONDecodeError as e:
        print(f"   Strategy 4 failed: {str(e)}")
    except Exception as e:
        print(f"   Strategy 4 error: {str(e)}")

    # Strategy 5: Extract JSON from specific patterns in your response
    print("🔄 Strategy 5: Trying response-specific extraction...")
    try:
        # Look for the specific pattern in your error: ```json\n{ ... }
        pattern = r'```json\s*\n(\{[\s\S]*)'
        match = re.search(pattern, content, re.IGNORECASE)
        if match:
            json_content = match.group(1)
            # Find the end of the JSON by counting braces
            brace_count = 0
            end_pos = 0
            in_string = False
            escape_next = False
            
            for i, char in enumerate(json_content):
                if escape_next:
                    escape_next = False
                    continue
                    
                if char == '\\':
                    escape_next = True
                    continue
                    
                if char == '"' and not escape_next:
                    in_string = not in_string
                    continue
                    
                if not in_string:
                    if char == '{':
                        brace_count += 1
                    elif char == '}':
                        brace_count -= 1
                        if brace_count == 0:
                            end_pos = i + 1
                            break
            
            if end_pos > 0:
                complete_json = json_content[:end_pos]
                print(f"   Extracted complete JSON (length: {len(complete_json)})")
                result = json.loads(complete_json)
                print("✅ Strategy 5: Response-specific extraction successful")
                return result
    except Exception as e:
        print(f"   Strategy 5 error: {str(e)}")

    # If all strategies fail, provide detailed error information
    content_preview = content[:1000] if len(content) > 1000 else content
    lines = content.split('\n')
    first_lines = '\n'.join(lines[:10])
    
    print("❌ All parsing strategies failed")
    print(f"Content starts with: {content[:100]}")
    print(f"Content ends with: {content[-100:]}")
    print(f"First 10 lines:\n{first_lines}")
    
    raise ValueError(f"Failed to parse JSON after all strategies. Content length: {len(content)}. Preview: {content_preview}...")


def extract_resume_data_from_response(parsed_response: Dict[str, Any]) -> Dict[str, Any]:
    """Extract resume data from potentially nested API response structure"""
    print("Extracting resume data from response structure...")
    
    # If it's already in the expected format, return as-is
    if 'personalInfo' in parsed_response or 'experience' in parsed_response:
        print("Response is already in resume data format")
        return parsed_response
    
    # Check for nested structures commonly returned by APIs
    possible_paths = [
        ['data', 'content'],      # {data: {content: {resumeData}}}
        ['content'],              # {content: {resumeData}}
        ['data'],                 # {data: {resumeData}}
        ['result'],               # {result: {resumeData}}
        ['response'],             # {response: {resumeData}}
    ]
    
    for path in possible_paths:
        try:
            current = parsed_response
            print(f"Trying path: {' -> '.join(path)}")
            
            for key in path:
                if isinstance(current, dict) and key in current:
                    current = current[key]
                    print(f"Found key '{key}', continuing...")
                else:
                    print(f"Key '{key}' not found, trying next path")
                    break
            else:
                # If we made it through the entire path
                if isinstance(current, dict) and ('personalInfo' in current or 'experience' in current):
                    print(f"Found resume data at path: {' -> '.join(path)}")
                    return current
                else:
                    print("Path complete but doesn't contain resume data")
        except Exception as e:
            print(f"Error following path {path}: {str(e)}")
            continue
    
    # Return original response if we can't extract resume data
    print("Could not extract resume data, returning original response")
    return parsed_response

def call_gemini_api_cached(resume_data: str, model: str = "gemini-2.5-flash-lite") -> Dict[str, Any]:
    """Higher-level function that returns analysis results and debug information"""
    try:
        # Build dynamic prompt
        dynamic_prompt = build_dynamic_prompt(resume_data)
        
        # Count input tokens
        input_tokens = count_tokens(dynamic_prompt, model)
        
        # Call API with caching
        analysis_content = call_gemini_with_cache_and_retry(dynamic_prompt, model, 2)
        
        # Count output tokens
        output_tokens = count_tokens(analysis_content, model)
        
        # Calculate cost
        total_cost = calculate_total_cost(input_tokens, output_tokens)
        
        # Get cache expiry info
        now = time.time() * 1000
        left_time_minutes = 0
        if resume_cache_expiry:
            left_time_minutes = max(round((resume_cache_expiry - now) / 60000), 0)
        
        return {
            "analysis_content": analysis_content,
            "debug_info": {
                "model": model,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "total_cost": total_cost,
                "prompt_length": len(dynamic_prompt),
                "resume_length": len(resume_data),
                "response_length": len(analysis_content),
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "resume_cache_id": resume_cache_id,
                "left_time_in_expiry_minutes": left_time_minutes,
            }
        }
        
    except Exception as e:
        print(f"Cached analysis failed: {str(e)}")
        raise e

def transform_text_to_resume_data_cached(raw_text: str) -> dict:
    """Transform text to structured data with Gemini API prompt caching"""
    print(f'Processing raw_text of length: {len(raw_text)}')
    
    # Check regular cache first (for text extraction results)
    text_hash = _get_text_hash(raw_text)
    cache_key = f"gemini_transform_cached:{text_hash}"

    cached_result = _get_from_cache(cache_key)
    if cached_result is not None:
        print("Using cached result")
        return {
            "success": True,
            "data": cached_result,
            "debug": {"cache_hit": True}
        }
        
    try:
        # Use cached API call
        print("Calling Gemini API with prompt caching...")
        api_response = call_gemini_api_cached(raw_text)
        
        # Extract the actual content from the response
        content = api_response.get("analysis_content")
        debug_info = api_response.get("debug_info", {})

        # Log debug information
        print(f"API Stats: {debug_info}")

        if not content:
            print("Empty response from API")
            return {
                "success": False,
                "error": "Gemini API returned empty content",
                "debug": debug_info
            }

        try:
            print("Parsing JSON response...")
            parsed_result = parse_gemini_response(content)
            result = extract_resume_data_from_response(parsed_result)
            print("Successfully parsed JSON response")
            
            if isinstance(result, dict):
                print(f"Final result has {len(result)} top-level keys")

            # Cache successful result
            _set_cache(cache_key, result)
            print("Result cached successfully")
            
            return {
                "success": True,
                "data": result,
                "debug": debug_info
            }

        except ValueError as e:
            print(f"JSON Parse Error: {str(e)}")
            return {
                "success": False,
                "error": f"Failed to parse JSON response: {str(e)}",
                "raw_content": content[:1000],
                "debug": debug_info
            }

    except Exception as e:
        print(f"Gemini API Exception: {str(e)}")
        return {
            "success": False,
            "error": f"Gemini API error: {str(e)}",
            "error_type": type(e).__name__,
            "debug": {"cache_hit": False}
        }

def parse_resume_cached(filename: str, content: bytes) -> dict:
    """Main parsing function with prompt caching - enhanced version"""
    # Check for complete cached result first
    file_hash = _get_file_hash(content)
    cache_key = f"full_parse_cached:{file_hash}"

    cached_result = _get_from_cache(cache_key)
    if cached_result is not None:
        print("Using cached complete parsing result")
        return {
            "success": True,
            "data": cached_result,
            "debug": {"cache_hit": True, "stage": "full_parse"}
        }

    # Process text extraction (with its own caching)
    if filename == 'resume.txt':
        raw_text = content.decode('utf-8') if isinstance(content, bytes) else content
    else:
        raw_text = extract_text_from_resume(filename, content)
    
    # Use cached API call for structured data transformation
    transform_result = transform_text_to_resume_data_cached(raw_text)

    if not transform_result.get("success"):
        # propagate failure but still include debug info
        return {
            "success": False,
            "error": transform_result.get("error", "Unknown error"),
            "raw_content": transform_result.get("raw_content"),
            "debug": {**transform_result.get("debug", {}), "stage": "transform"}
        }

    structured_data = transform_result["data"]

    # Cache the complete result
    _set_cache(cache_key, structured_data)
    return {
        "success": True,
        "data": structured_data,
        "debug": {**transform_result.get("debug", {}), "stage": "full_parse"}
    }

