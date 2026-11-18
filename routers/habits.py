"""Daily habits for the Streaks screen, stored as one JSON file in R2.

The browser decides what "today" is (it knows the user's time zone) and sends
dates as YYYY-MM-DD; streaks are counted from those dates on the client.
"""
import json
import re
import threading
import uuid
from datetime import date

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from services.storage import r2_client, R2_BUCKET_NAME

router = APIRouter()
_lock = threading.Lock()
ICONS = {"cards", "book", "language", "pencil", "video", "pdf", "mic", "run", "water", "star"}
MAX_HABITS = 50


class HabitCreate(BaseModel):
    name: str
    icon: str = "star"


class HabitRename(BaseModel):
    name: str
    icon: str | None = None


class HabitCheck(BaseModel):
    date: str
    done: bool = True


def _key() -> str:
    return f"{R2_BUCKET_NAME}/habits/index.json"


def _require_storage():
    if not r2_client or not R2_BUCKET_NAME:
        raise HTTPException(status_code=400, detail="Cloudflare R2 is not configured")


def _load() -> list[dict]:
    try:
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=_key())
        data = json.loads(obj["Body"].read().decode("utf-8"))
    except r2_client.exceptions.NoSuchKey:
        return []
    except Exception:
        raise HTTPException(status_code=502, detail="Couldn't load your habits. Try again.")
    habits = data.get("habits") if isinstance(data, dict) else None
    return habits if isinstance(habits, list) else []


def _save(habits: list[dict]) -> None:
    try:
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME, Key=_key(), ContentType="application/json",
            Body=json.dumps({"habits": habits}, ensure_ascii=False).encode("utf-8"),
        )
    except Exception:
        raise HTTPException(status_code=502, detail="Couldn't save your habits. Try again.")


def _clean_name(name: str) -> str:
    name = re.sub(r"\s+", " ", name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Enter a habit name.")
    if len(name) > 60:
        raise HTTPException(status_code=400, detail="Keep the habit name under 60 characters.")
    return name


def _clean_icon(icon: str | None) -> str:
    return icon if icon in ICONS else "star"


def _clean_date(value: str) -> str:
    try:
        return date.fromisoformat(value).isoformat()
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Send the date as YYYY-MM-DD.")


def _find(habits: list[dict], habit_id: str) -> dict:
    for habit in habits:
        if habit.get("id") == habit_id:
            return habit
    raise HTTPException(status_code=404, detail="That habit no longer exists.")


@router.get("/habits")
def list_habits():
    _require_storage()
    return {"habits": _load()}


@router.post("/habits")
def create_habit(body: HabitCreate):
    _require_storage()
    name = _clean_name(body.name)
    with _lock:
        habits = _load()
        if len(habits) >= MAX_HABITS:
            raise HTTPException(status_code=400, detail=f"You can track up to {MAX_HABITS} habits.")
        if any(h.get("name", "").lower() == name.lower() for h in habits):
            raise HTTPException(status_code=400, detail="You already have a habit with that name.")
        habit = {"id": uuid.uuid4().hex[:12], "name": name, "icon": _clean_icon(body.icon),
                 "created": date.today().isoformat(), "done": []}
        habits.append(habit)
        _save(habits)
    return habit


@router.post("/habits/{habit_id}/check")
def check_habit(habit_id: str, body: HabitCheck):
    _require_storage()
    day = _clean_date(body.date)
    with _lock:
        habits = _load()
        habit = _find(habits, habit_id)
        done = set(d for d in habit.get("done", []) if isinstance(d, str))
        if body.done:
            done.add(day)
        else:
            done.discard(day)
        habit["done"] = sorted(done)
        _save(habits)
    return habit


@router.post("/habits/{habit_id}/rename")
def rename_habit(habit_id: str, body: HabitRename):
    _require_storage()
    name = _clean_name(body.name)
    with _lock:
        habits = _load()
        habit = _find(habits, habit_id)
        if any(h is not habit and h.get("name", "").lower() == name.lower() for h in habits):
            raise HTTPException(status_code=400, detail="You already have a habit with that name.")
        habit["name"] = name
        if body.icon is not None:
            habit["icon"] = _clean_icon(body.icon)
        _save(habits)
    return habit


@router.delete("/habits/{habit_id}")
def delete_habit(habit_id: str):
    _require_storage()
    with _lock:
        habits = _load()
        _find(habits, habit_id)
        _save([h for h in habits if h.get("id") != habit_id])
    return {"ok": True}
