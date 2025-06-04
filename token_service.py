# token_service.py
import jwt
import os
from typing import Dict, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def verify_token(token: str) -> Dict[str, Any]:
    """
    Verify JWT token and return decoded payload
    
    Args:
        token (str): JWT token to verify
        
    Returns:
        Dict[str, Any]: Decoded token payload
        
    Raises:
        Exception: If token is invalid, expired, or verification fails
    """
    try:
        # Get JWT secret key from environment variables
        secret_key = os.getenv("JWT_SECRET_KEY")
        if not secret_key:
            raise Exception("JWT_SECRET_KEY environment variable is required")
        
        # Get algorithm from environment or use default
        algorithm = os.getenv("JWT_ALGORITHM", "HS256")
        
        # Decode and verify the token
        decoded_token = jwt.decode(
            token, 
            secret_key, 
            algorithms=[algorithm],
            verify_exp=True,  # Verify expiration
            verify_aud=False,  # Set to True if you use audience claims
            verify_iss=False   # Set to True if you use issuer claims
        )
        
        logger.info(f"Token verified successfully for user: {decoded_token.get('userId', 'unknown')}")
        return decoded_token
        
    except jwt.ExpiredSignatureError:
        logger.warning("Token has expired")
        raise Exception("Token has expired")
    except jwt.InvalidTokenError as e:
        logger.warning(f"Invalid token: {str(e)}")
        raise Exception(f"Invalid token: {str(e)}")
    except Exception as e:
        logger.error(f"Token verification failed: {str(e)}")
        raise Exception(f"Token verification failed: {str(e)}")