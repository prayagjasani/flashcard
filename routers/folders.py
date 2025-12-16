import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from fastapi import APIRouter, HTTPException
from botocore.exceptions import ClientError

from models import FolderCreate, FolderRename, FolderDelete, FolderMove, FolderOrderUpdate
from services.storage import r2_client, R2_BUCKET_NAME
from services.cache import get_cached, set_cached, invalidate_cache
from utils import safe_deck_name as _safe_deck_name

router = APIRouter()

# Cache TTL in seconds
CACHE_TTL = 30

# Key for the single folders file (combines index and order)
def _folders_index_key() -> str:
    return f"{R2_BUCKET_NAME}/folders/index.json"


def _fetch_deck_index():
    """Fetch csv/index.json from R2 (with caching)."""
    cache_key = "folders:deck_index"
    cached = get_cached(cache_key, CACHE_TTL)
    if cached is not None:
        return cached
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=f"{R2_BUCKET_NAME}/csv/index.json")
        data = obj["Body"].read().decode("utf-8")
        result = json.loads(data)
        set_cached(cache_key, result)
        return result
    except Exception:
        return []


def _fetch_folders_index():
    """Fetch folders/index.json from R2 (with caching).
    
    This single file now serves as both the list of folders AND their display order.
    The order of items in the array determines display order.
    """
    cache_key = "folders:folders_index"
    cached = get_cached(cache_key, CACHE_TTL)
    if cached is not None:
        return cached
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=_folders_index_key())
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        result = parsed if isinstance(parsed, list) else []
        set_cached(cache_key, result)
        return result
    except Exception:
        return []


def _fetch_parents():
    """Fetch folders/parents.json from R2 (with caching)."""
    cache_key = "folders:parents"
    cached = get_cached(cache_key, CACHE_TTL)
    if cached is not None:
        return cached
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=f"{R2_BUCKET_NAME}/folders/parents.json")
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        result = parsed if isinstance(parsed, dict) else {}
        set_cached(cache_key, result)
        return result
    except Exception:
        return {}


@router.get("/folders")
def get_folders():
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    
    # Parallel R2 fetches (now only 3 instead of 4)
    deck_index = []
    folders_index = []  # This is now the ordered list
    parents_data = {}
    
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(_fetch_deck_index): "deck_index",
            executor.submit(_fetch_folders_index): "folders_index",
            executor.submit(_fetch_parents): "parents",
        }
        for future in as_completed(futures):
            key = futures[future]
            try:
                result = future.result()
                if key == "deck_index":
                    deck_index = result if isinstance(result, list) else []
                elif key == "folders_index":
                    folders_index = result if isinstance(result, list) else []
                elif key == "parents":
                    parents_data = result if isinstance(result, dict) else {}
            except Exception:
                pass
    
    # Count decks per folder
    counts = {}
    folders_from_decks = set()
    for d in deck_index:
        if isinstance(d, dict):
            f = d.get("folder") or "Uncategorized"
            folders_from_decks.add(f)
            counts[f] = counts.get(f, 0) + 1
    
    # Build ordered list from folders_index (preserving order)
    ordered = []
    seen = set()
    
    # First, add folders in the order they appear in folders_index
    for f in folders_index:
        if isinstance(f, str) and f not in seen:
            ordered.append({
                "name": f, 
                "count": counts.get(f, 0), 
                "parent": parents_data.get(f)
            })
            seen.add(f)
    
    # Then add any folders that exist in decks but not in the index (e.g., "Uncategorized")
    for f in sorted(folders_from_decks):
        if f not in seen:
            ordered.append({
                "name": f, 
                "count": counts.get(f, 0), 
                "parent": parents_data.get(f)
            })
            seen.add(f)
    
    return {"folders": ordered}


@router.post("/folder/create")
def folder_create(payload: FolderCreate):
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    name = _safe_deck_name(payload.name)
    if not name:
        raise HTTPException(status_code=400, detail="Folder name required")
    
    key = _folders_index_key()
    items = []
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        if isinstance(parsed, list):
            items = parsed
    except Exception:
        pass
    
    # Append new folder at the end (preserving order)
    if name not in items:
        items.append(name)
    
    r2_client.put_object(
        Bucket=R2_BUCKET_NAME, 
        Key=key, 
        Body=json.dumps(items).encode("utf-8"), 
        ContentType="application/json"
    )
    invalidate_cache("folders:")
    return {"ok": True, "name": name}


@router.post("/folder/rename")
def folder_rename(payload: FolderRename):
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    old = _safe_deck_name(payload.old_name)
    new = _safe_deck_name(payload.new_name)
    if not old or not new:
        raise HTTPException(status_code=400, detail="Folder name required")
    
    key = _folders_index_key()
    items = []
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        if isinstance(parsed, list):
            items = parsed
    except Exception:
        pass
    
    # Rename in place to preserve order
    if old in items:
        items = [new if x == old else x for x in items]
    elif new not in items:
        items.append(new)
    
    r2_client.put_object(
        Bucket=R2_BUCKET_NAME, 
        Key=key, 
        Body=json.dumps(items).encode("utf-8"), 
        ContentType="application/json"
    )
    
    # Update deck index (folder references in decks)
    idx_key = f"{R2_BUCKET_NAME}/csv/index.json"
    try:
        idx_obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=idx_key)
        idx_data = idx_obj["Body"].read().decode("utf-8")
        parsed = json.loads(idx_data)
        if isinstance(parsed, list):
            for d in parsed:
                if isinstance(d, dict) and (d.get("folder") or "") == old:
                    d["folder"] = new
            r2_client.put_object(
                Bucket=R2_BUCKET_NAME, 
                Key=idx_key, 
                Body=json.dumps(parsed).encode("utf-8"), 
                ContentType="application/json"
            )
    except Exception:
        pass
    
    # Update folder parents when renaming
    parents_key = f"{R2_BUCKET_NAME}/folders/parents.json"
    try:
        parents_data = {}
        try:
            pobj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=parents_key)
            pdata = pobj["Body"].read().decode("utf-8")
            parsed_p = json.loads(pdata)
            if isinstance(parsed_p, dict):
                parents_data = parsed_p
        except Exception:
            pass
        
        updated = False
        # If the renamed folder had a parent, update its key
        if old in parents_data:
            parents_data[new] = parents_data.pop(old)
            updated = True
        # If any folder had old as parent, update to new
        for k, v in list(parents_data.items()):
            if v == old:
                parents_data[k] = new
                updated = True
        if updated:
            r2_client.put_object(
                Bucket=R2_BUCKET_NAME, 
                Key=parents_key, 
                Body=json.dumps(parents_data).encode("utf-8"), 
                ContentType="application/json"
            )
    except Exception:
        pass
    
    invalidate_cache("folders:")
    return {"ok": True, "old_name": old, "new_name": new}


@router.delete("/folder/delete")
def folder_delete(payload: FolderDelete):
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    name = _safe_deck_name(payload.name)
    if not name:
        raise HTTPException(status_code=400, detail="Folder name required")
    
    key = _folders_index_key()
    items = []
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        if isinstance(parsed, list):
            items = [x for x in parsed if x != name]
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME, 
            Key=key, 
            Body=json.dumps(items).encode("utf-8"), 
            ContentType="application/json"
        )
    except Exception:
        pass
    
    # Update deck index (remove folder from decks)
    idx_key = f"{R2_BUCKET_NAME}/csv/index.json"
    try:
        idx_obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=idx_key)
        idx_data = idx_obj["Body"].read().decode("utf-8")
        parsed = json.loads(idx_data)
        if isinstance(parsed, list):
            for d in parsed:
                if isinstance(d, dict) and (d.get("folder") or "") == name:
                    d.pop("folder", None)
            r2_client.put_object(
                Bucket=R2_BUCKET_NAME, 
                Key=idx_key, 
                Body=json.dumps(parsed).encode("utf-8"), 
                ContentType="application/json"
            )
    except Exception:
        pass
    
    # Clean up folder parents when deleting
    parents_key = f"{R2_BUCKET_NAME}/folders/parents.json"
    try:
        parents_data = {}
        try:
            pobj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=parents_key)
            pdata = pobj["Body"].read().decode("utf-8")
            parsed_p = json.loads(pdata)
            if isinstance(parsed_p, dict):
                parents_data = parsed_p
        except Exception:
            pass
        
        updated = False
        # Remove the deleted folder's parent entry
        if name in parents_data:
            del parents_data[name]
            updated = True
        # Remove parent reference for any child folders (move them to root)
        for k, v in list(parents_data.items()):
            if v == name:
                del parents_data[k]
                updated = True
        if updated:
            r2_client.put_object(
                Bucket=R2_BUCKET_NAME, 
                Key=parents_key, 
                Body=json.dumps(parents_data).encode("utf-8"), 
                ContentType="application/json"
            )
    except Exception:
        pass
    
    invalidate_cache("folders:")
    return {"ok": True, "deleted": name}


@router.post("/folder/move")
def folder_move(payload: FolderMove):
    """Move a folder to be a child of another folder (nested folders)."""
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    name = _safe_deck_name(payload.name)
    if not name:
        raise HTTPException(status_code=400, detail="Folder name required")
    parent = _safe_deck_name(payload.parent) if payload.parent else None
    
    # Prevent moving folder into itself or its descendants
    if parent and parent == name:
        raise HTTPException(status_code=400, detail="Cannot move folder into itself")
    
    # Read folder parents data
    parents_key = f"{R2_BUCKET_NAME}/folders/parents.json"
    parents_data = {}
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=parents_key)
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        if isinstance(parsed, dict):
            parents_data = parsed
    except Exception:
        pass
    
    # Check for circular reference: walk up from parent to ensure name is not an ancestor
    if parent:
        current = parent
        visited = set()
        while current:
            if current == name:
                raise HTTPException(status_code=400, detail="Cannot move folder into its own descendant")
            if current in visited:
                break
            visited.add(current)
            current = parents_data.get(current)
    
    # Update parent
    if parent:
        parents_data[name] = parent
    else:
        parents_data.pop(name, None)
    
    r2_client.put_object(
        Bucket=R2_BUCKET_NAME,
        Key=parents_key,
        Body=json.dumps(parents_data).encode("utf-8"),
        ContentType="application/json"
    )
    
    invalidate_cache("folders:")
    return {"ok": True, "name": name, "parent": parent}


@router.get("/order/folders")
def order_folders_get():
    """Get the folder order (same as folders index since they are combined)."""
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    
    # Check cache first
    cache_key = "folders:folders_index"
    cached = get_cached(cache_key, CACHE_TTL)
    if cached is not None:
        return cached
    
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=_folders_index_key())
        data = obj["Body"].read().decode("utf-8")
        arr = json.loads(data)
        if isinstance(arr, list):
            set_cached(cache_key, arr)
            return arr
        return []
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return []
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/order/folders")
def order_folders_set(payload: FolderOrderUpdate):
    """Set the folder order (updates the combined index file)."""
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    
    names = [_safe_deck_name(x) for x in (payload.order or []) if _safe_deck_name(x)]
    
    try:
        # Save the new order to the single folders index file
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME, 
            Key=_folders_index_key(), 
            Body=json.dumps(names).encode("utf-8"), 
            ContentType="application/json"
        )
        invalidate_cache("folders:")
        return {"ok": True, "order": names}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
