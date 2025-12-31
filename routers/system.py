import io
import os
import re
import json
import csv
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from gtts import gTTS
from botocore.exceptions import ClientError

from models import AudioRebuildRequest
from services.storage import (
    r2_client, R2_BUCKET_NAME, R2_ENDPOINT, 
    R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_PUBLIC_URL_BASE,
    lines_key as _lines_key
)
from utils import safe_tts_key as _safe_tts_key_util, safe_deck_name as _safe_deck_name

router = APIRouter()

# Debug mode from environment
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() in ("true", "1", "yes")

# Allowed R2 key prefixes for public access
ALLOWED_KEY_PREFIXES = [
    "tts/",
    "csv/",
    "lines/",
    "stories/",
    "order/",
    "folders/",
    "pdf/",
]

def _safe_tts_key(text: str, lang: str = "de") -> str:
    return _safe_tts_key_util(text, R2_BUCKET_NAME, lang)

@router.get("/r2/health")
def r2_health():
    """Simple health endpoint for R2 configuration diagnostics (no secrets)."""
    return {
        "configured": bool(r2_client and R2_BUCKET_NAME),
        "client_initialized": bool(r2_client is not None),
        "has_access_key": bool(R2_ACCESS_KEY_ID),
        "has_secret_key": bool(R2_SECRET_ACCESS_KEY),
        "has_endpoint": bool(R2_ENDPOINT),
        "has_bucket": bool(R2_BUCKET_NAME),
        "endpoint": R2_ENDPOINT,
        "bucket": R2_BUCKET_NAME,
        "public_url_base": R2_PUBLIC_URL_BASE,
    }

@router.get("/debug/r2-config")
def debug_r2_config():
    """Debug endpoint to check R2 configuration in deployment.
    
    Protected: Only available when DEBUG_MODE=true in environment.
    """
    if not DEBUG_MODE:
        raise HTTPException(status_code=403, detail="Debug endpoints are disabled in production")
    
    config_info = {
        "r2_configured": bool(r2_client and R2_BUCKET_NAME),
        "bucket_name": R2_BUCKET_NAME,
        "endpoint": R2_ENDPOINT,
        "has_credentials": bool(R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY),
        "account_id": "HIDDEN" if R2_ACCESS_KEY_ID else None,
    }
    
    # Test basic R2 connection
    if r2_client and R2_BUCKET_NAME:
        try:
            response = r2_client.list_objects_v2(
                Bucket=R2_BUCKET_NAME,
                Prefix=f"{R2_BUCKET_NAME}/csv/",
                MaxKeys=5
            )
            config_info["r2_connection"] = "success"
            config_info["objects_found"] = len(response.get("Contents", []))
            config_info["sample_keys"] = [obj["Key"] for obj in response.get("Contents", [])[:3]]
        except Exception as e:
            config_info["r2_connection"] = "failed"
            config_info["r2_error"] = str(e)
    else:
        config_info["r2_connection"] = "not_configured"
    
    return config_info

@router.get("/r2/get")
def r2_get(key: str):
    """Stream an object from Cloudflare R2 by key.
    
    Key must start with an allowed prefix for security.
    """
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    
    # Validate key format and prevent path traversal
    if ".." in key or key.startswith("/"):
        raise HTTPException(status_code=400, detail="Invalid key format")
    
    # Extract the path after bucket name prefix
    key_path = key
    if key.startswith(f"{R2_BUCKET_NAME}/"):
        key_path = key[len(f"{R2_BUCKET_NAME}/"):]
    
    # Check if key is in allowed prefixes
    is_allowed = any(key_path.startswith(prefix) for prefix in ALLOWED_KEY_PREFIXES)
    if not is_allowed:
        raise HTTPException(status_code=403, detail="Access to this key is not allowed")
    
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        stream = obj["Body"]
        content_type = obj.get("ContentType", "application/octet-stream")
        return StreamingResponse(stream, media_type=content_type)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            raise HTTPException(status_code=404, detail="Object not found")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/audio/cleanup")
def audio_cleanup(dry_run: bool = False):
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    try:
        valid_texts = set()
        idx_key = f"{R2_BUCKET_NAME}/csv/index.json"
        decks = []
        try:
            idx_obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=idx_key)
            idx_data = idx_obj["Body"].read().decode("utf-8")
            parsed = json.loads(idx_data)
            if isinstance(parsed, list):
                decks = parsed
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code not in ("404", "NoSuchKey", "NotFound"):
                raise HTTPException(status_code=500, detail=str(e))
        except Exception:
            decks = []

        for d in decks:
            if not isinstance(d, dict):
                continue
            name = d.get("name") or ""
            file_key = d.get("file") or f"{R2_BUCKET_NAME}/csv/{_safe_deck_name(name)}.csv"
            try:
                obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=file_key)
                data = obj["Body"].read().decode("utf-8")
                reader = csv.reader(io.StringIO(data))
                for row in reader:
                    if len(row) >= 2:
                        de = (row[1] or "").strip()
                        if de:
                            valid_texts.add(de)
            except Exception:
                pass
            try:
                lkey = _lines_key(name)
                lobj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=lkey)
                ldata = lobj["Body"].read().decode("utf-8")
                lparsed = json.loads(ldata)
                items = lparsed.get("items") if isinstance(lparsed, dict) else lparsed
                items = items or []
                for it in items:
                    if isinstance(it, dict):
                        t = (it.get("line_de") or "").strip()
                        if t:
                            valid_texts.add(t)
            except Exception:
                pass

        valid_keys = set(_safe_tts_key(t, "de") for t in valid_texts)
        prefix = f"{R2_BUCKET_NAME}/tts/de/"
        continuation = None
        total = 0
        deleted = 0
        kept = 0
        errors = 0
        while True:
            kwargs = {"Bucket": R2_BUCKET_NAME, "Prefix": prefix}
            if continuation:
                kwargs["ContinuationToken"] = continuation
            resp = r2_client.list_objects_v2(**kwargs)
            contents = resp.get("Contents", [])
            for obj in contents:
                key = obj.get("Key", "")
                if not key.endswith(".mp3"):
                    continue
                total += 1
                if key in valid_keys:
                    kept += 1
                else:
                    if dry_run:
                        deleted += 1
                    else:
                        try:
                            r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=key)
                            deleted += 1
                        except Exception:
                            errors += 1
            if resp.get("IsTruncated"):
                continuation = resp.get("NextContinuationToken")
            else:
                break

        return {
            "ok": True,
            "dry_run": dry_run,
            "tts_total": total,
            "kept": kept,
            "deleted": deleted,
            "errors": errors,
            "valid_texts": len(valid_texts),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to cleanup audio: {e}")
