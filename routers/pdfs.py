import json
from datetime import datetime, timezone
from io import BytesIO

from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from botocore.exceptions import ClientError
from PIL import Image
try:
    import pypdfium2 as pdfium
except Exception:  # pragma: no cover
    pdfium = None

from models import (
    PdfRename,
    PdfDelete,
    PdfMove,
    PdfOrderUpdate,
    PdfFolderCreate,
    PdfFolderRename,
    PdfFolderDelete,
    PdfFolderMove,
)
from services.storage import r2_client, R2_BUCKET_NAME, order_pdfs_key as _order_pdfs_key
from services.cache import get_cached, set_cached, invalidate_cache
from utils import safe_deck_name as _safe_name


router = APIRouter()

PDF_ORDER_CACHE_TTL = 30


def _pdf_index_key() -> str:
    return f"{R2_BUCKET_NAME}/pdf/index.json"


def _pdf_folders_index_key() -> str:
    return f"{R2_BUCKET_NAME}/pdf/folders/index.json"


def _thumb_key(name: str) -> str:
    return f"{R2_BUCKET_NAME}/pdf/thumbs/{name}.jpg"


def _build_thumb(content: bytes, safe_name: str) -> str | None:
    if not r2_client or not R2_BUCKET_NAME or pdfium is None:
        return None
    if not content:
        return None
    try:
        doc = pdfium.PdfDocument(BytesIO(content))
        if len(doc) == 0:
            return None
        page = doc[0]
        image = page.render(scale=2.0).to_pil()
        image.thumbnail((800, 800), Image.LANCZOS)
        buf = BytesIO()
        image.save(buf, format="JPEG", quality=90)
        data = buf.getvalue()
        key = _thumb_key(safe_name)
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=key,
            Body=data,
            ContentType="image/jpeg",
        )
        return key
    except Exception:
        return None


@router.get("/pdf/folders")
def get_pdf_folders():
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    pdf_index: list[dict] = []
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=_pdf_index_key())
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        if isinstance(parsed, list):
            pdf_index = [d for d in parsed if isinstance(d, dict)]
    except Exception:
        pdf_index = []
    counts: dict[str, int] = {}
    folders_from_pdfs: set[str] = set()
    for d in pdf_index:
        f = d.get("folder") or "Uncategorized"
        if not isinstance(f, str):
            continue
        folders_from_pdfs.add(f)
        counts[f] = counts.get(f, 0) + 1
    index_names: list[str] = []
    key = _pdf_folders_index_key()
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        if isinstance(parsed, list):
            index_names = [x for x in parsed if isinstance(x, str)]
    except Exception:
        index_names = []
    ordered: list[dict] = []
    seen: set[str] = set()
    for name in index_names:
        if name in seen:
            continue
        ordered.append({"name": name, "count": counts.get(name, 0), "parent": None})
        seen.add(name)
    for name in sorted(folders_from_pdfs):
        if name in seen:
            continue
        ordered.append({"name": name, "count": counts.get(name, 0), "parent": None})
        seen.add(name)
    if "Uncategorized" not in seen:
        ordered.append({"name": "Uncategorized", "count": counts.get("Uncategorized", 0), "parent": None})
    return {"folders": ordered}


@router.post("/pdf/folder/create")
def pdf_folder_create(payload: PdfFolderCreate):
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    name = _safe_name(payload.name)
    if not name:
        raise HTTPException(status_code=400, detail="Folder name required")
    key = _pdf_folders_index_key()
    items: list[str] = []
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        if isinstance(parsed, list):
            items = [x for x in parsed if isinstance(x, str)]
    except Exception:
        items = []
    if name not in items:
        items.append(name)
    try:
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=key,
            Body=json.dumps(items).encode("utf-8"),
            ContentType="application/json",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    invalidate_cache("pdfs:folders")
    return {"ok": True, "name": name}


@router.post("/pdf/folder/rename")
def pdf_folder_rename(payload: PdfFolderRename):
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    old = _safe_name(payload.old_name)
    new = _safe_name(payload.new_name)
    if not old or not new:
        raise HTTPException(status_code=400, detail="Folder name required")
    key = _pdf_folders_index_key()
    items: list[str] = []
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        if isinstance(parsed, list):
            items = [x for x in parsed if isinstance(x, str)]
    except Exception:
        items = []
    changed = False
    if old in items:
        items = [new if x == old else x for x in items]
        changed = True
    elif new not in items:
        items.append(new)
        changed = True
    if changed:
        try:
            r2_client.put_object(
                Bucket=R2_BUCKET_NAME,
                Key=key,
                Body=json.dumps(items).encode("utf-8"),
                ContentType="application/json",
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    index_key = _pdf_index_key()
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=index_key)
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        if isinstance(parsed, list):
            updated = False
            for d in parsed:
                if isinstance(d, dict) and (d.get("folder") or "") == old:
                    d["folder"] = new
                    updated = True
            if updated:
                r2_client.put_object(
                    Bucket=R2_BUCKET_NAME,
                    Key=index_key,
                    Body=json.dumps(parsed).encode("utf-8"),
                    ContentType="application/json",
                )
    except Exception:
        pass
    invalidate_cache("pdfs:")
    return {"ok": True, "old_name": old, "new_name": new}


@router.delete("/pdf/folder/delete")
def pdf_folder_delete(payload: PdfFolderDelete):
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    name = _safe_name(payload.name)
    if not name:
        raise HTTPException(status_code=400, detail="Folder name required")
    key = _pdf_folders_index_key()
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        items: list[str] = []
        if isinstance(parsed, list):
            items = [x for x in parsed if isinstance(x, str) and x != name]
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=key,
            Body=json.dumps(items).encode("utf-8"),
            ContentType="application/json",
        )
    except Exception:
        pass
    index_key = _pdf_index_key()
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=index_key)
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        if isinstance(parsed, list):
            updated = False
            for d in parsed:
                if isinstance(d, dict) and (d.get("folder") or "") == name:
                    d.pop("folder", None)
                    updated = True
            if updated:
                r2_client.put_object(
                    Bucket=R2_BUCKET_NAME,
                    Key=index_key,
                    Body=json.dumps(parsed).encode("utf-8"),
                    ContentType="application/json",
                )
    except Exception:
        pass
    invalidate_cache("pdfs:")
    return {"ok": True, "deleted": name}


@router.post("/pdf/folder/move")
def pdf_folder_move(payload: PdfFolderMove):
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    source = _safe_name(payload.source)
    if not source:
        raise HTTPException(status_code=400, detail="Source folder required")
    target = _safe_name(payload.target) if payload.target else None
    key = _pdf_folders_index_key()
    items: list[str] = []
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        if isinstance(parsed, list):
            items = [x for x in parsed if isinstance(x, str)]
    except Exception:
        items = []
    changed_index = False
    if target and target not in items:
        items.append(target)
        changed_index = True
    if changed_index:
        try:
            r2_client.put_object(
                Bucket=R2_BUCKET_NAME,
                Key=key,
                Body=json.dumps(items).encode("utf-8"),
                ContentType="application/json",
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    index_key = _pdf_index_key()
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=index_key)
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        if isinstance(parsed, list):
            updated = False
            for d in parsed:
                if isinstance(d, dict) and (d.get("folder") or "") == source:
                    if target:
                        d["folder"] = target
                    else:
                        d.pop("folder", None)
                    updated = True
            if updated:
                r2_client.put_object(
                    Bucket=R2_BUCKET_NAME,
                    Key=index_key,
                    Body=json.dumps(parsed).encode("utf-8"),
                    ContentType="application/json",
                )
    except Exception:
        pass
    invalidate_cache("pdfs:")
    return {"ok": True, "source": source, "target": target}


@router.get("/pdfs")
def list_pdfs():
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=_pdf_index_key())
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        if isinstance(parsed, list):
            items = []
            for d in parsed:
                if isinstance(d, dict):
                    name = d.get("name")
                    file = d.get("file")
                    folder = d.get("folder")
                    lm = d.get("last_modified")
                    thumb = d.get("thumb")
                    if name and file and file.lower().endswith(".pdf"):
                        items.append(
                            {
                                "name": name,
                                "file": file,
                                "folder": folder,
                                "last_modified": lm,
                                "thumb": thumb,
                            }
                        )
            try:
                if not any(it.get("last_modified") for it in items):
                    lm_map = {}
                    continuation = None
                    while True:
                        kwargs = {"Bucket": R2_BUCKET_NAME, "Prefix": f"{R2_BUCKET_NAME}/pdf/"}
                        if continuation:
                            kwargs["ContinuationToken"] = continuation
                        resp = r2_client.list_objects_v2(**kwargs)
                        for o in resp.get("Contents", []):
                            lm = o.get("LastModified")
                            lm_map[o.get("Key", "")] = lm.isoformat() if lm else ""
                        if resp.get("IsTruncated"):
                            continuation = resp.get("NextContinuationToken")
                        else:
                            break
                    for it in items:
                        file = it.get("file", "")
                        full_key = file if "/pdf/" in file else f"{R2_BUCKET_NAME}/{file}"
                        it["last_modified"] = lm_map.get(full_key, it.get("last_modified", ""))
            except Exception:
                pass
            items.sort(key=lambda x: x.get("last_modified") or "", reverse=True)
            return items
        return []
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return []
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/pdf/upload")
async def upload_pdf(
    name: str = Form(...),
    folder: str | None = Form(None),
    file: UploadFile = File(...),
):
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    safe_name = _safe_name(name)
    if not safe_name:
        raise HTTPException(status_code=400, detail="PDF name required")
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty PDF")
    key = f"{R2_BUCKET_NAME}/pdf/{safe_name}.pdf"
    thumb_key = _build_thumb(content, safe_name)
    try:
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=key,
            Body=content,
            ContentType="application/pdf",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    index_key = _pdf_index_key()
    items = []
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=index_key)
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        if isinstance(parsed, list):
            items = parsed
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code not in ("404", "NoSuchKey", "NotFound"):
            raise HTTPException(status_code=500, detail=str(e))
    except Exception:
        pass
    now_iso = datetime.now(timezone.utc).isoformat()
    safe_folder = _safe_name(folder) if folder else None
    updated = False
    for d in items:
        if isinstance(d, dict) and d.get("name") == safe_name:
            d["file"] = key
            if thumb_key:
                d["thumb"] = thumb_key
            if safe_folder:
                d["folder"] = safe_folder
            elif "folder" in d:
                d.pop("folder", None)
            d["last_modified"] = now_iso
            updated = True
            break
    if not updated:
        entry = {"name": safe_name, "file": key, "last_modified": now_iso}
        if thumb_key:
            entry["thumb"] = thumb_key
        if safe_folder:
            entry["folder"] = safe_folder
        items.append(entry)
    try:
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=index_key,
            Body=json.dumps(items).encode("utf-8"),
            ContentType="application/json",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    scope = _safe_name(folder) if folder else "root"
    invalidate_cache(f"pdfs:order:{scope}")
    return {"ok": True, "name": safe_name, "file": key, "folder": safe_folder}


@router.post("/pdf/rename")
def rename_pdf(payload: PdfRename):
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    old = _safe_name(payload.old_name)
    new = _safe_name(payload.new_name)
    if not old or not new:
        raise HTTPException(status_code=400, detail="PDF name required")
    if old == new:
        raise HTTPException(status_code=400, detail="New name must be different")
    old_key = f"{R2_BUCKET_NAME}/pdf/{old}.pdf"
    new_key = f"{R2_BUCKET_NAME}/pdf/{new}.pdf"
    try:
        r2_client.head_object(Bucket=R2_BUCKET_NAME, Key=new_key)
        raise HTTPException(status_code=400, detail="Target PDF already exists")
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code not in ("404", "NoSuchKey", "NotFound"):
            raise HTTPException(status_code=500, detail=str(e))
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=old_key)
        content = obj["Body"].read()
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            raise HTTPException(status_code=404, detail="PDF not found")
        raise HTTPException(status_code=500, detail=str(e))
    try:
        r2_client.put_object(Bucket=R2_BUCKET_NAME, Key=new_key, Body=content, ContentType="application/pdf")
        r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=old_key)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    index_key = _pdf_index_key()
    items = []
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=index_key)
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        if isinstance(parsed, list):
            items = parsed
    except Exception:
        pass
    now_iso = datetime.now(timezone.utc).isoformat()
    folders = set()
    for d in items:
        if isinstance(d, dict) and d.get("name") == old:
            d["name"] = new
            d["file"] = new_key
            if "thumb" in d and d["thumb"]:
                old_thumb = d["thumb"]
                try:
                    obj_t = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=old_thumb)
                    tcontent = obj_t["Body"].read()
                    new_thumb_key = _thumb_key(new)
                    r2_client.put_object(
                        Bucket=R2_BUCKET_NAME,
                        Key=new_thumb_key,
                        Body=tcontent,
                        ContentType="image/jpeg",
                    )
                    d["thumb"] = new_thumb_key
                except Exception:
                    d.pop("thumb", None)
            d["last_modified"] = now_iso
            if d.get("folder"):
                folders.add(_safe_name(d.get("folder")))
    try:
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=index_key,
            Body=json.dumps(items).encode("utf-8"),
            ContentType="application/json",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    for f in folders or {"root"}:
        invalidate_cache(f"pdfs:order:{f or 'root'}")
    return {"ok": True, "old_name": old, "new_name": new}


@router.delete("/pdf/delete")
def delete_pdf(payload: PdfDelete):
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    name = _safe_name(payload.name)
    if not name:
        raise HTTPException(status_code=400, detail="PDF name required")
    key = f"{R2_BUCKET_NAME}/pdf/{name}.pdf"
    thumb_key = _thumb_key(name)
    try:
        r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=key)
        try:
            r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=thumb_key)
        except ClientError:
            pass
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code not in ("404", "NoSuchKey", "NotFound"):
            raise HTTPException(status_code=500, detail=str(e))
    index_key = _pdf_index_key()
    items = []
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=index_key)
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        if isinstance(parsed, list):
            items = parsed
    except Exception:
        pass
    folders = set()
    new_items = []
    for d in items:
        if isinstance(d, dict) and d.get("name") == name:
            if d.get("folder"):
                folders.add(_safe_name(d.get("folder")))
            continue
        new_items.append(d)
    try:
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=index_key,
            Body=json.dumps(new_items).encode("utf-8"),
            ContentType="application/json",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    for f in folders or {"root"}:
        scope = f or "root"
        try:
            okey = _order_pdfs_key(scope)
            obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=okey)
            data = obj["Body"].read().decode("utf-8")
            parsed = json.loads(data)
            if isinstance(parsed, list):
                new_order = [x for x in parsed if x != name]
                r2_client.put_object(
                    Bucket=R2_BUCKET_NAME,
                    Key=okey,
                    Body=json.dumps(new_order).encode("utf-8"),
                    ContentType="application/json",
                )
        except Exception:
            pass
        invalidate_cache(f"pdfs:order:{scope}")
    return {"ok": True, "name": name}


@router.post("/pdf/move")
def move_pdf(payload: PdfMove):
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    name = _safe_name(payload.name)
    if not name:
        raise HTTPException(status_code=400, detail="PDF name required")
    folder = _safe_name(payload.folder) if payload.folder else None
    index_key = _pdf_index_key()
    items = []
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=index_key)
        data = obj["Body"].read().decode("utf-8")
        parsed = json.loads(data)
        if isinstance(parsed, list):
            items = parsed
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    prev_folder = None
    for d in items:
        if isinstance(d, dict) and d.get("name") == name:
            prev_folder = d.get("folder") or None
            if folder:
                d["folder"] = folder
            elif "folder" in d:
                d.pop("folder", None)
    try:
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=index_key,
            Body=json.dumps(items).encode("utf-8"),
            ContentType="application/json",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    try:
        if prev_folder:
            pkey = _order_pdfs_key(prev_folder)
            plist = []
            try:
                pobj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=pkey)
                pdata = pobj["Body"].read().decode("utf-8")
                parsed_p = json.loads(pdata)
                if isinstance(parsed_p, list):
                    plist = parsed_p
            except Exception:
                pass
            if name in plist:
                plist = [x for x in plist if x != name]
                r2_client.put_object(
                    Bucket=R2_BUCKET_NAME,
                    Key=pkey,
                    Body=json.dumps(plist).encode("utf-8"),
                    ContentType="application/json",
                )
        tkey = _order_pdfs_key(folder or "root")
        tlist = []
        try:
            tobj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=tkey)
            tdata = tobj["Body"].read().decode("utf-8")
            parsed_t = json.loads(tdata)
            if isinstance(parsed_t, list):
                tlist = parsed_t
        except Exception:
            pass
        if name not in tlist:
            tlist.append(name)
            r2_client.put_object(
                Bucket=R2_BUCKET_NAME,
                Key=tkey,
                Body=json.dumps(tlist).encode("utf-8"),
                ContentType="application/json",
            )
    except Exception:
        pass
    if prev_folder:
        invalidate_cache(f"pdfs:order:{_safe_name(prev_folder)}")
    invalidate_cache(f"pdfs:order:{_safe_name(folder or 'root')}")
    return {"ok": True, "name": name, "folder": folder or None}


@router.get("/order/pdfs")
def order_pdfs_get(scope: str | None = None):
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    safe_scope = _safe_name(scope or "root") or "root"
    cache_key = f"pdfs:order:{safe_scope}"
    cached = get_cached(cache_key, PDF_ORDER_CACHE_TTL)
    if cached is not None:
        return cached
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=_order_pdfs_key(scope))
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


@router.post("/order/pdfs")
def order_pdfs_set(payload: PdfOrderUpdate):
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")
    scope = _safe_name(payload.scope or "root") or "root"
    names = [_safe_name(x) for x in (payload.order or []) if _safe_name(x)]
    try:
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=_order_pdfs_key(scope),
            Body=json.dumps(names).encode("utf-8"),
            ContentType="application/json",
        )
        invalidate_cache(f"pdfs:order:{scope}")
        return {"ok": True, "scope": scope, "order": names}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
