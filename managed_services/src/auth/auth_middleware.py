from fastapi import HTTPException, Request
from typing import Dict, Any, Optional
import logging
from .token_service import verify_token, get_auth_user_id

logger = logging.getLogger(__name__)


async def extract_and_verify_token(request: Request) -> Dict[str, Any]:
    """
    Extract and verify JWT token from Authorization header

    Args:
        request: FastAPI request object

    Returns:
        Dict[str, Any]: Decoded token payload with user info

    Raises:
        HTTPException: If authentication fails
    """

    # Check Authorization header
    auth_header = request.headers.get("authorization")
    if not auth_header:
        logger.warning("Missing Authorization header")
        raise HTTPException(
            status_code=400,
            detail={"error": "Authorization header is missing", "status": 400}
        )

    # Extract token from Bearer format
    try:
        token = auth_header.split(" ")[1]
    except IndexError:
        logger.warning("Invalid Authorization header format")
        raise HTTPException(
            status_code=400,
            detail={"error": "Bearer token missing", "status": 400}
        )

    # Verify token
    try:
        decoded_token = await verify_token(token)
        user_id = get_auth_user_id(decoded_token)

        # Return enhanced payload with user info
        return {
            "userId": user_id,
            "decoded_token": decoded_token,
            "is_authenticated": True
        }

    except Exception as err:
        logger.warning(f"Token verification failed: {str(err)}")
        raise HTTPException(
            status_code=401,
            detail={"error": str(err), "status": 401}
        )


def require_auth(allow_anonymous: bool = False):
    """
    Decorator factory for endpoints that require authentication

    Args:
        allow_anonymous: If True, allows unauthenticated access but still extracts user info if token is present

    Returns:
        Function that extracts user authentication info
    """

    async def auth_dependency(request: Request) -> Optional[Dict[str, Any]]:
        """
        FastAPI dependency that extracts and validates authentication

        Returns:
            Dict with user info if authenticated, None if anonymous allowed and no token
        """

        if allow_anonymous:
            # Try to extract token but don't fail if missing
            auth_header = request.headers.get("authorization")
            if not auth_header:
                return None

            try:
                return await extract_and_verify_token(request)
            except HTTPException:
                return None
        else:
            # Require valid authentication
            return await extract_and_verify_token(request)

    return auth_dependency


# Common dependency instances
require_authentication = require_auth(allow_anonymous=False)
optional_authentication = require_auth(allow_anonymous=True)