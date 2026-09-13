"""Normalized card library. Media continues to live in R2.

Schema creation/import is explicit; request handlers never initialize an empty DB.
"""
import csv
import io
import os
from contextlib import contextmanager
from functools import lru_cache

from fastapi import HTTPException
from sqlalchemy import (MetaData, Table, Column, Integer, Text, ForeignKey,
                        UniqueConstraint, CheckConstraint, create_engine, select,
                        func, text, delete, update)
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.engine import make_url
from sqlalchemy.pool import NullPool
from utils import safe_deck_name

metadata = MetaData()
folders = Table('fc_folders', metadata,
    Column('id', Integer, primary_key=True), Column('name', Text, nullable=False, unique=True),
    Column('parent_id', Integer, ForeignKey('fc_folders.id', ondelete='SET NULL'), index=True),
    Column('position', Integer, nullable=False, default=0),
    CheckConstraint('parent_id IS NULL OR parent_id <> id'))
decks = Table('fc_decks', metadata,
    Column('id', Integer, primary_key=True), Column('name', Text, nullable=False, unique=True),
    Column('folder_id', Integer, ForeignKey('fc_folders.id', ondelete='SET NULL'), index=True),
    Column('position', Integer, nullable=False, default=0),
    Column('last_modified', Text, nullable=True))
cards = Table('fc_cards', metadata,
    Column('id', Integer, primary_key=True),
    Column('deck_id', Integer, ForeignKey('fc_decks.id', ondelete='CASCADE'), nullable=False, index=True),
    Column('position', Integer, nullable=False), Column('en', Text, nullable=False),
    Column('de', Text, nullable=False), UniqueConstraint('deck_id', 'position'))
state = Table('fc_state', metadata, Column('key', Text, primary_key=True), Column('value', Text, nullable=False))


def init_schema(drop_existing=False):
    eng = engine()
    if drop_existing:
        metadata.drop_all(eng)
    metadata.create_all(eng)


def enabled():
    return os.getenv('FLASHCARD_STORAGE', 'r2').lower() == 'postgres'


@lru_cache(maxsize=1)
def engine():
    raw = next((os.getenv(key) for key in ('NEON_DATABASE_URL', 'NEON_URL', 'DATABASE_URL') if os.getenv(key)), None)
    if not raw:
        raise HTTPException(503, 'PostgreSQL connection is not configured')
    try:
        url = make_url(raw).set(drivername='postgresql+psycopg')
        # Neon already pools connections; avoid idle app-side pools on serverless hosts.
        return create_engine(url, poolclass=NullPool, connect_args={'connect_timeout': 10}, hide_parameters=True)
    except Exception:
        raise HTTPException(503, 'Invalid PostgreSQL configuration') from None


def normalized(name):
    name = safe_deck_name(name or '')
    if not name:
        raise HTTPException(400, 'Name is required')
    return name


def parse_csv(content):
    try:
        rows = list(csv.reader(io.StringIO(content.lstrip('\ufeff')), strict=True))
    except csv.Error:
        raise HTTPException(400, 'Invalid CSV content') from None
    result = []
    for number, row in enumerate(rows, 1):
        if not row or not any(cell.strip() for cell in row):
            continue
        if len(row) != 2 or not all(cell.strip() for cell in row):
            raise HTTPException(400, f'CSV row {number} must contain English and German text')
        result.append({'en': row[0].strip(), 'de': row[1].strip()})
    return result


@contextmanager
def transaction(write=False):
    try:
        with engine().begin() as conn:
            if write and conn.dialect.name == 'postgresql':
                conn.execute(text('SELECT pg_advisory_xact_lock(74318021)'))
            if conn.execute(select(state.c.value).where(state.c.key == 'ready')).scalar() != '1':
                raise HTTPException(503, 'PostgreSQL library has not been imported and verified')
            yield conn
    except IntegrityError:
        raise HTTPException(409, 'Name already exists or a referenced item changed') from None
    except SQLAlchemyError:
        raise HTTPException(503, 'PostgreSQL is unavailable; no fallback write was made') from None


def find(conn, table, name):
    row = conn.execute(select(table).where(table.c.name == normalized(name))).mappings().first()
    if row is None:
        raise HTTPException(404, 'Folder not found' if table is folders else 'Deck not found')
    return row


def folder_id(conn, name):
    return None if not name or name in ('root', 'Uncategorized') else find(conn, folders, name)['id']


def get_cards(name):
    with transaction() as conn:
        deck = find(conn, decks, name)
        return [dict(row) for row in conn.execute(select(cards.c.en, cards.c.de).where(cards.c.deck_id == deck['id']).order_by(cards.c.position)).mappings()]


def library():
    with transaction() as conn:
        fs = [dict(row) for row in conn.execute(select(folders).order_by(folders.c.position, folders.c.id)).mappings()]
        ds = [dict(row) for row in conn.execute(select(decks).order_by(decks.c.position, decks.c.id)).mappings()]
    names = {f['id']: f['name'] for f in fs}
    result_folders = [{'id': f['id'], 'name': f['name'], 'parent': names.get(f['parent_id']),
                       'count': sum(d['folder_id'] == f['id'] for d in ds)} for f in fs]
    result_folders.append({'name': 'Uncategorized', 'parent': None, 'count': sum(d['folder_id'] is None for d in ds)})
    return {'folders': result_folders, 'folder_order': [f['name'] for f in result_folders],
            'decks': [{'id': d['id'], 'name': d['name'], 'folder': names.get(d['folder_id']),
                       'file': f"csv/{d['name']}.csv", 'last_modified': d['last_modified']} for d in ds]}


def export_csv(name):
    content = io.StringIO()
    csv.writer(content).writerows((c['en'], c['de']) for c in get_cards(name))
    return {'name': normalized(name), 'file': f'csv/{normalized(name)}.csv', 'csv': content.getvalue()}


def check_parent(conn, item_id, parent_id):
    seen = {item_id}
    while parent_id is not None:
        if parent_id in seen:
            raise HTTPException(400, 'Cannot move a folder into itself or its descendants')
        seen.add(parent_id)
        parent_id = conn.execute(select(folders.c.parent_id).where(folders.c.id == parent_id)).scalar()


def folder_change(action, payload):
    with transaction(write=True) as conn:
        name = normalized(getattr(payload, 'name', None) or getattr(payload, 'old_name', None))
        if name == 'Uncategorized':
            raise HTTPException(400, 'Uncategorized is a virtual folder')
        if action == 'create':
            parent = folder_id(conn, payload.parent)
            position = (conn.execute(select(func.max(folders.c.position))).scalar() or 0) + 1
            conn.execute(folders.insert().values(name=name, parent_id=parent, position=position))
        else:
            item = find(conn, folders, name)
            if action == 'delete':
                conn.execute(delete(folders).where(folders.c.id == item['id']))
            elif action == 'rename':
                new = normalized(payload.new_name)
                if new == 'Uncategorized':
                    raise HTTPException(400, 'Reserved folder name')
                conn.execute(update(folders).where(folders.c.id == item['id']).values(name=new))
            elif action == 'move':
                parent = folder_id(conn, payload.parent)
                check_parent(conn, item['id'], parent)
                conn.execute(update(folders).where(folders.c.id == item['id']).values(parent_id=parent))
    return {'ok': True, 'name': name, 'deleted': name, 'new_name': getattr(payload, 'new_name', None)}


def deck_change(action, payload):
    from datetime import datetime, timezone
    with transaction(write=True) as conn:
        name = normalized(getattr(payload, 'name', None) or getattr(payload, 'old_name', None))
        if action in ('create', 'update'):
            rows = parse_csv(payload.data if action == 'create' else payload.content)
            if action == 'create' and not rows:
                raise HTTPException(400, 'No valid cards found')
            now = datetime.now(timezone.utc).isoformat()
            if action == 'create':
                fid = folder_id(conn, payload.folder)
                position = (conn.execute(select(func.max(decks.c.position))).scalar() or 0) + 1
                did = conn.execute(decks.insert().values(name=name, folder_id=fid, position=position, last_modified=now).returning(decks.c.id)).scalar_one()
            else:
                did = find(conn, decks, name)['id']
                conn.execute(update(decks).where(decks.c.id == did).values(last_modified=now))
                conn.execute(delete(cards).where(cards.c.deck_id == did))
            if rows:
                conn.execute(cards.insert(), [dict(deck_id=did, position=i, **row) for i, row in enumerate(rows)])
        else:
            item = find(conn, decks, name)
            if action == 'delete':
                conn.execute(delete(decks).where(decks.c.id == item['id']))
            elif action == 'rename':
                conn.execute(update(decks).where(decks.c.id == item['id']).values(name=normalized(payload.new_name)))
            elif action == 'move':
                pos = (conn.execute(select(func.max(decks.c.position))).scalar() or 0) + 1
                conn.execute(update(decks).where(decks.c.id == item['id']).values(folder_id=folder_id(conn, payload.folder), position=pos))
    # Shared audio is kept: deleting it would affect other decks using the same word.
    return {'ok': True, 'name': name, 'rows': len(rows) if action in ('create', 'update') else 0,
            'index_updated': True, 'audio_status': 'available_on_demand'}


def move_bulk(payload):
    with transaction(write=True) as conn:
        target = folder_id(conn, payload.folder)
        ids = [find(conn, decks, name)['id'] for name in dict.fromkeys(payload.names)]
        pos = (conn.execute(select(func.max(decks.c.position))).scalar() or 0) + 1
        for i, did in enumerate(ids):
            conn.execute(update(decks).where(decks.c.id == did).values(folder_id=target, position=pos+i))
    return {'ok': True, 'count': len(ids)}


def order(kind, names=None, scope=None):
    table = folders if kind == 'folders' else decks
    with transaction(write=names is not None) as conn:
        stmt = select(table.c.id, table.c.name).order_by(table.c.position, table.c.id)
        if kind == 'decks':
            stmt = stmt.where(decks.c.folder_id == folder_id(conn, scope))
        existing = list(conn.execute(stmt).mappings())
        available = {row['name']: row['id'] for row in existing}
        if names is not None:
            chosen = list(dict.fromkeys(normalized(n) for n in names if n != 'Uncategorized'))
            if any(n not in available for n in chosen):
                raise HTTPException(400, 'Order contains items outside this folder')
            chosen += [n for n in available if n not in chosen]
            for i, name in enumerate(chosen):
                conn.execute(update(table).where(table.c.id == available[name]).values(position=i))
            return {'ok': True, 'order': chosen, 'scope': scope}
        return list(available) + (['Uncategorized'] if kind == 'folders' else [])
