import asyncio
import json
import time
import os
from typing import Dict, Any, Optional
from src.parsers.text_extractor import extract_text
from src.parsers.gemini_normalizer import normalize_with_gemini
from src.parsers.gemini_cached_normalizer import normalize_with_gemini_cached


async def process_resume(file_content: bytes, filename: str) -> Dict[str, Any]:
    """
    Main processing pipeline for resume parsing

    Flow: File → Text Extraction → Gemini Normalization → Structured Response
    Target: 15-second response time for production use
    """
    start_time = time.time()

    try:
        # Step 1: Extract text based on file type (8-12 seconds)
        print(f"Starting text extraction for {filename}...")
        raw_text = await extract_text(file_content, filename)

        if not raw_text or len(raw_text.strip()) < 50:
            raise ValueError(f"Insufficient text extracted from {filename}")

        extraction_time = time.time() - start_time
        print(f"Text extraction completed in {extraction_time:.2f}s")

        # Step 2: Normalize with Gemini (3-5 seconds)
        # Choose caching mode based on environment configuration
        use_caching = os.getenv("USE_PROMPT_CACHING", "false").lower() == "true"

        if use_caching:
            print("Starting Gemini normalization with caching (89% cost reduction)...")
            normalization_start = time.time()
            structured_data = await normalize_with_gemini_cached(raw_text)
        else:
            print("Starting Gemini normalization (standard mode)...")
            normalization_start = time.time()
            structured_data = await normalize_with_gemini(raw_text)

        normalization_time = time.time() - normalization_start
        cache_mode = "cached" if use_caching else "standard"
        print(f"Gemini normalization ({cache_mode}) completed in {normalization_time:.2f}s")

        # Step 3: Add processing metadata
        total_time = time.time() - start_time
        result = add_processing_metadata(structured_data, filename, total_time, len(raw_text))

        print(f"Total processing time: {total_time:.2f}s")
        return result

    except Exception as e:
        total_time = time.time() - start_time
        print(f"Error processing {filename}: {str(e)}")
        return create_error_response(str(e), filename, total_time)


def add_processing_metadata(data: Dict[str, Any], filename: str, processing_time: float, text_length: int) -> Dict[str, Any]:
    """Add metadata to successful processing results"""

    file_extension = filename.lower().split('.')[-1] if '.' in filename else 'unknown'

    return {
        "success": True,
        "data": data,
        "metadata": {
            "filename": filename,
            "file_type": file_extension,
            "processing_time_seconds": round(processing_time, 2),
            "extracted_text_length": text_length,
            "processing_method": get_processing_method(filename),
            "timestamp": time.time()
        }
    }


def create_error_response(error_message: str, filename: str, processing_time: float) -> Dict[str, Any]:
    """Create standardized error response"""

    empty_structure = get_empty_resume_structure()
    # Update the parseMetadata with error info
    empty_structure["data"]["parseMetadata"]["parseTime"] = processing_time
    empty_structure["data"]["parseMetadata"]["warnings"] = [
        {
            "type": "processing_error",
            "message": error_message,
            "section": "all",
            "field": "",
            "severity": "high"
        }
    ]

    return empty_structure


def get_processing_method(filename: str) -> str:
    """Determine processing method based on filename"""

    ext = filename.lower().split('.')[-1] if '.' in filename else ''

    if ext in ['jpg', 'jpeg', 'png']:
        return 'aws_textract_ocr'
    elif ext == 'pdf':
        return 'aws_textract_pdf'
    elif ext in ['doc', 'docx']:
        return 'python_docx'
    elif ext == 'txt':
        return 'direct_text'
    else:
        return 'unknown'


def get_empty_resume_structure() -> Dict[str, Any]:
    """Return empty structure matching static_prompt.py format for error cases"""

    content_data = {
        "personalInfo": {
            "fullName": "",
            "title": "",
            "email": "",
            "phone": "",
            "location": "",
            "linkedIn": "",
            "portfolio": "",
            "github": "",
            "customLinks": []
        },
        "summary": {
            "content": ""
        },
        "experience": [],
        "education": [],
        "skills": {
            "extracted": []
        }
    }

    return {
        "success": False,
        "data": {
            "content": content_data,
            "parseMetadata": {
                "confidence": 0.0,
                "parseTime": 0.0,
                "detectedSections": [],
                "missingSections": ["personalInfo", "experience", "education", "skills"],
                "sectionConfidence": {
                    "personalInfo": 0.0,
                    "experience": 0.0,
                    "education": 0.0,
                    "skills": 0.0,
                    "projects": 0.0
                },
                "warnings": [],
                "suggestions": [],
                "extractedKeywords": [],
                "industryDetected": "",
                "experienceLevel": "Entry",
                "totalExperienceYears": 0,
                "educationLevel": "",
                "atsKeywords": {
                    "technical": [],
                    "soft": [],
                    "industry": [],
                    "certifications": []
                },
                "stats": {
                    "totalWords": 0,
                    "bulletPoints": 0,
                    "quantifiedAchievements": 0,
                    "actionVerbs": 0,
                    "uniqueSkills": 0
                }
            }
        }
    }


async def validate_resume_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Validate and clean the resume data structure"""

    # Ensure all required fields exist
    if "personalInfo" not in data:
        data["personalInfo"] = {}

    # Validate personalInfo fields
    required_personal_fields = ["name", "email", "phone", "location", "linkedin", "github", "portfolio"]
    for field in required_personal_fields:
        if field not in data["personalInfo"]:
            data["personalInfo"][field] = ""

    # Ensure arrays exist
    if "experience" not in data or not isinstance(data["experience"], list):
        data["experience"] = []

    if "education" not in data or not isinstance(data["education"], list):
        data["education"] = []

    if "skills" not in data:
        data["skills"] = {"technical": [], "languages": [], "certifications": []}

    if "summary" not in data:
        data["summary"] = ""

    return data


def calculate_confidence_score(data: Dict[str, Any], text_length: int) -> float:
    """Calculate confidence score based on extracted data completeness"""

    score = 0.0
    total_weight = 1.0

    # Personal info completeness (40% weight)
    personal_info = data.get("personalInfo", {})
    personal_fields_filled = sum(1 for field in ["name", "email", "phone"] if personal_info.get(field, "").strip())
    score += (personal_fields_filled / 3) * 0.4

    # Experience data (30% weight)
    experience = data.get("experience", [])
    if experience and len(experience) > 0:
        score += 0.3

    # Education data (20% weight)
    education = data.get("education", [])
    if education and len(education) > 0:
        score += 0.2

    # Text length factor (10% weight)
    if text_length > 500:
        score += 0.1
    elif text_length > 200:
        score += 0.05

    return min(round(score, 2), 1.0)