import json
from fastapi import APIRouter, HTTPException
from botocore.exceptions import ClientError

from models import FolderCreate, FolderRename, FolderDelete, FolderMove, FolderOrderUpdate
from services.storage import (
    r2_client, R2_BUCKET_NAME, 
    order_folders_key as _order_folders_key
)
from utils import safe_deck_name as _safe_deck_name

router = APIRouter()

@router.get("/folders")
def get_folders():
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    index_key = f"{R2_BUCKET_NAME}/csv/index.json"
    folders_key = f"{R2_BUCKET_NAME}/folders/index.json"
    parents_key = f"{R2_BUCKET_NAME}/folders/parents.json"
    names = set()
    counts = {}
    try:
        idx_obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=index_key)
        idx_data = idx_obj["Body"].read().decode("utf-8")
        parsed = json.loads(idx_data)
        if isinstance(parsed, list):
            for d in parsed:
                if isinstance(d, dict):
                    f = d.get("folder") or "Uncategorized"
                    names.add(f)
                    counts[f] = counts.get(f, 0) + 1
    except Exception:
        pass
    extra = []
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=folders_key)
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        if isinstance(parsed, list):
            extra = [str(x) for x in parsed]
    except Exception:
        pass
    for f in extra:
        names.add(f)
        counts.setdefault(f, 0)
    # Load parent relationships
    parents_data = {}
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=parents_key)
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        if isinstance(parsed, dict):
            parents_data = parsed
    except Exception:
        pass
    base = [{"name": n, "count": counts.get(n, 0), "parent": parents_data.get(n)} for n in names]
    ordered = base
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=_order_folders_key())
        data = obj["Body"].read().decode("utf-8")
        arr = json.loads(data)
        if isinstance(arr, list):
            name_to_item = {x["name"]: x for x in base}
            ordered = [name_to_item[n] for n in arr if n in name_to_item]
            for x in base:
                if x["name"] not in arr:
                    ordered.append(x)
    except Exception:
        ordered = sorted(base, key=lambda x: x["name"].lower())
    return {"folders": ordered}

@router.post("/folder/create")
def folder_create(payload: FolderCreate):
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    name = _safe_deck_name(payload.name)
    if not name:
        raise HTTPException(status_code=400, detail="Folder name required")
    key = f"{R2_BUCKET_NAME}/folders/index.json"
    items = []
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        if isinstance(parsed, list):
            items = parsed
    except Exception:
        pass
    if name not in items:
        items.append(name)
    r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=key, Body=json.dumps(items).encode("utf-8"), ContentType="application/json")
    return {"ok": True, "name": name}

@router.post("/folder/rename")
def folder_rename(payload: FolderRename):
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    old = _safe_deck_name(payload.old_name)
    new = _safe_deck_name(payload.new_name)
    if not old or not new:
        raise HTTPException(status_code=400, detail="Folder name required")
    key = f"{R2_BUCKET_NAME}/folders/index.json"
    items = []
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        if isinstance(parsed, list):
            items = parsed
    except Exception:
        pass
    if old in items:
        items = [new if x == old else x for x in items]
    if new not in items:
        items.append(new)
    r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=key, Body=json.dumps(items).encode("utf-8"), ContentType="application/json")
    # Update folders order list if present
    try:
        ok = False
        okey = _order_folders_key()
        oitems = []
        try:
            oobj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=okey)
            odata = oobj["Body"].read().decode("utf-8")
            parsed_o = json.loads(odata)
            if isinstance(parsed_o, list):
                oitems = parsed_o
        except Exception:
            pass
        if old in oitems:
            oitems = [new if x == old else x for x in oitems]
            r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=okey, Body=json.dumps(oitems).encode("utf-8"), ContentType="application/json")
            ok = True
    except Exception:
        pass
    idx_key = f"{R2_BUCKET_NAME}/csv/index.json"
    try:
        idx_obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=idx_key)
        idx_data = idx_obj["Body"].read().decode("utf-8")
        parsed = json.loads(idx_data)
        if isinstance(parsed, list):
            for d in parsed:
                if isinstance(d, dict) and (d.get("folder") or "") == old:
                    d["folder"] = new
            r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=idx_key, Body=json.dumps(parsed).encode("utf-8"), ContentType="application/json")
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
            r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=parents_key, Body=json.dumps(parents_data).encode("utf-8"), ContentType="application/json")
    except Exception:
        pass
    return {"ok": True, "old_name": old, "new_name": new}

@router.post("/folder/delete")
def folder_delete(payload: FolderDelete):
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    name = _safe_deck_name(payload.name)
    if not name:
        raise HTTPException(status_code=400, detail="Folder name required")
    key = f"{R2_BUCKET_NAME}/folders/index.json"
    items = []
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        if isinstance(parsed, list):
            items = [x for x in parsed if x != name]
        r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=key, Body=json.dumps(items).encode("utf-8"), ContentType="application/json")
    except Exception:
        pass
    # Remove from folders order if present
    try:
        okey = _order_folders_key()
        oitems = []
        try:
            oobj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=okey)
            odata = oobj["Body"].read().decode("utf-8")
            parsed_o = json.loads(odata)
            if isinstance(parsed_o, list):
                oitems = parsed_o
        except Exception:
            pass
        if name in oitems:
            oitems = [x for x in oitems if x != name]
            r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=okey, Body=json.dumps(oitems).encode("utf-8"), ContentType="application/json")
    except Exception:
        pass
    idx_key = f"{R2_BUCKET_NAME}/csv/index.json"
    try:
        idx_obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=idx_key)
        idx_data = idx_obj["Body"].read().decode("utf-8")
        parsed = json.loads(idx_data)
        if isinstance(parsed, list):
            for d in parsed:
                if isinstance(d, dict) and (d.get("folder") or "") == name:
                    d.pop("folder", None)
            r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=idx_key, Body=json.dumps(parsed).encode("utf-8"), ContentType="application/json")
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
            r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=parents_key, Body=json.dumps(parents_data).encode("utf-8"), ContentType="application/json")
    except Exception:
        pass
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
    
    return {"ok": True, "name": name, "parent": parent}

@router.get("/order/folders")
def order_folders_get():
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=_order_folders_key())
        data = obj["Body"].read().decode("utf-8")
        arr = json.loads(data)
        if isinstance(arr, list):
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
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    names = [ _safe_deck_name(x) for x in (payload.order or []) if _safe_deck_name(x) ]
    try:
        r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=_order_folders_key(), Body=json.dumps(names).encode("utf-8"), ContentType="application/json")
        return {"ok": True, "order": names}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
