"""Shared deck service to eliminate code duplication across routers."""

import csv
import io
from fastapi import HTTPException
from botocore.exceptions import ClientError

from services.storage import r2_client, R2_BUCKET_NAME
from utils import safe_deck_name


def get_cards(deck: str) -> list[dict]:
    """
    Fetch cards from a deck CSV in R2.
    
    Args:
        deck: The deck name
        
    Returns:
        List of card dictionaries with 'en' and 'de' keys
        
    Raises:
        HTTPException: If deck name is invalid, not found, or R2 error occurs
    """
    safe = safe_deck_name(deck)
    if not safe:
        raise HTTPException(status_code=400, detail="Invalid deck name")

    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    
    key = f"{R2_BUCKET_NAME}/csv/{safe}.csv"
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        data = obj["Body"].read().decode("utf-8")
        result = []
        reader = csv.reader(io.StringIO(data))
        for row in reader:
            if len(row) >= 2:
                en, de = row[0].strip(), row[1].strip()
                if en and de:
                    result.append({"en": en, "de": de})
        return result
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            raise HTTPException(status_code=404, detail="Deck not found")
        raise HTTPException(status_code=500, detail=str(e))


def get_cards_silent(deck: str) -> list[dict]:
    """
    Fetch cards from a deck CSV in R2, returning empty list on errors.
    
    Useful for background operations where exceptions shouldn't propagate.
    
    Args:
        deck: The deck name
        
    Returns:
        List of card dictionaries, or empty list on any error
    """
    try:
        return get_cards(deck)
    except Exception:
        return []
