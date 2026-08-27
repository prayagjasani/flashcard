"""Build a dependency graph for the source files in this project."""

from __future__ import annotations

import ast
import re
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SUPPORTED_SUFFIXES = {".py", ".html", ".js", ".css", ".json", ".txt"}
SUPPORTED_NAMES = {"Dockerfile", "requirements.txt"}
IGNORED_PARTS = {".git", ".vscode", "__pycache__", "node_modules", ".venv", "venv"}

ROUTE_RE = re.compile(r"@router\.(?:get|post|put|patch|delete|head)\(\s*['\"]([^'\"]+)")
WEB_REF_RE = re.compile(r"(?:src|href)\s*=\s*['\"]([^'\"?#]+)|fetch\(\s*[`'\"]([^`'\"?#]+)", re.I)
FILE_RESPONSE_RE = re.compile(r"FileResponse\(\s*['\"]([^'\"]+)")


def _project_files() -> list[Path]:
    files = []
    for path in PROJECT_ROOT.rglob("*"):
        if not path.is_file() or any(part in IGNORED_PARTS for part in path.relative_to(PROJECT_ROOT).parts):
            continue
        if path.suffix.lower() in SUPPORTED_SUFFIXES or path.name in SUPPORTED_NAMES:
            files.append(path)
    return sorted(files, key=lambda item: item.relative_to(PROJECT_ROOT).as_posix().lower())


def _kind(path: Path) -> str:
    return {
        ".py": "python", ".html": "html", ".js": "javascript",
        ".css": "css", ".json": "config", ".txt": "config",
    }.get(path.suffix.lower(), "config")


def _resolve_python_module(module: str) -> str | None:
    candidate = PROJECT_ROOT.joinpath(*module.split("."))
    for path in (candidate.with_suffix(".py"), candidate / "__init__.py"):
        if path.is_file():
            return path.relative_to(PROJECT_ROOT).as_posix()
    return None


def _python_edges(source: str, text: str) -> list[tuple[str, str]]:
    edges = []
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return edges
    for node in ast.walk(tree):
        modules: list[str] = []
        if isinstance(node, ast.Import):
            modules = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules = [node.module]
            # `from routers import screens` points at the child file, not only the package.
            modules.extend(f"{node.module}.{alias.name}" for alias in node.names)
        for module in modules:
            target = _resolve_python_module(module)
            if target and target != source:
                edges.append((target, "imports"))
    return edges


def _route_records(source: str, text: str) -> list[dict]:
    """Extract FastAPI route metadata, including a template returned by a screen route."""
    records = []
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return records
    for node in tree.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        template = None
        for child in ast.walk(node):
            if (
                isinstance(child, ast.Call)
                and isinstance(child.func, ast.Name)
                and child.func.id == "FileResponse"
                and child.args
                and isinstance(child.args[0], ast.Constant)
                and isinstance(child.args[0].value, str)
            ):
                template = Path(child.args[0].value).as_posix()
                break
        for decorator in node.decorator_list:
            if not (
                isinstance(decorator, ast.Call)
                and isinstance(decorator.func, ast.Attribute)
                and decorator.func.attr in {"get", "post", "put", "patch", "delete", "head"}
                and decorator.args
                and isinstance(decorator.args[0], ast.Constant)
            ):
                continue
            records.append({
                "path": decorator.args[0].value,
                "method": decorator.func.attr.upper(),
                "owner": source,
                "handler": node.name,
                "template": template,
            })
    return records


def _local_web_path(value: str) -> str | None:
    if not value.startswith("/"):
        return None
    path = value.lstrip("/")
    candidate = PROJECT_ROOT / path
    if candidate.is_file():
        return candidate.relative_to(PROJECT_ROOT).as_posix()
    return None


def _file_insight(path: str, text: str) -> dict:
    """Return small, deterministic explanations suitable for the architecture UI."""
    name = Path(path).name
    symbols: list[str] = []
    if path.endswith(".py"):
        try:
            tree = ast.parse(text)
            symbols = [
                node.name for node in tree.body
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
            ][:12]
        except SyntaxError:
            pass

    exact_purposes = {
        "app.py": "Application entry point. It creates FastAPI, configures middleware, registers every router, and serves static files.",
        "models.py": "Defines the validated request-data shapes shared by API endpoints.",
        "utils.py": "Contains small reusable helpers for safe names, keys, and hashes.",
        "requirements.txt": "Lists the Python packages required to run the backend.",
        "Dockerfile": "Describes how the application is packaged and started in a container.",
        "static/js/index.js": "Runs the main browser experience: navigation, deck/folder UI, flashcards, audio, and API requests.",
        "templates/index.html": "The main browser screen and its UI structure.",
        "services/storage.py": "Owns Cloudflare R2 configuration and low-level object-storage access.",
        "services/ai.py": "Talks to the AI model and turns vocabulary or subtitles into generated learning content.",
        "services/audio.py": "Generates, caches, and stores speech audio used by learning activities.",
        "services/cache.py": "Keeps frequently used data in memory so repeated requests are faster.",
        "services/executor.py": "Runs slow background work away from request-handling threads.",
        "services/deck_service.py": "Provides reusable deck-reading logic for multiple routers.",
        "services/project_graph.py": "Scans this repository and builds the architecture data shown on this page.",
        "routers/screens.py": "Maps browser URLs to their HTML screen files and exposes the project graph API.",
    }
    purpose = exact_purposes.get(path)
    if not purpose and path.startswith("routers/"):
        subject = name.removesuffix(".py").replace("_", " ")
        purpose = f"Handles HTTP requests for {subject} features, validates input, and coordinates services or storage."
    elif not purpose and path.startswith("services/"):
        subject = name.removesuffix(".py").replace("_", " ")
        purpose = f"Reusable backend logic for {subject}; routers call this instead of duplicating the work."
    elif not purpose and path.startswith("templates/"):
        screen = name.removesuffix(".html").replace("_", " ")
        purpose = f"Defines the {screen} screen that is rendered in the browser."
    elif not purpose and path.startswith("static/js/"):
        purpose = "Browser-side behavior that responds to user actions and communicates with backend APIs."
    elif not purpose and path.startswith("static/css/"):
        purpose = "Controls the visual appearance and responsive layout of browser screens."
    elif not purpose:
        purpose = "Project configuration or supporting source used by the application."
    return {"purpose": purpose, "symbols": symbols}


def build_project_graph() -> dict:
    files = _project_files()
    ids = {path.relative_to(PROJECT_ROOT).as_posix() for path in files}
    texts: dict[str, str] = {}
    routes: dict[str, set[str]] = {}
    route_records: list[dict] = []

    for path in files:
        relative = path.relative_to(PROJECT_ROOT).as_posix()
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            text = ""
        texts[relative] = text
        if path.suffix == ".py":
            for route in ROUTE_RE.findall(text):
                routes.setdefault(route, set()).add(relative)
            route_records.extend(_route_records(relative, text))

    edge_set: set[tuple[str, str, str]] = set()
    for source, text in texts.items():
        if source.endswith(".py"):
            for target, relation in _python_edges(source, text):
                if target in ids:
                    edge_set.add((source, target, relation))
            for target in FILE_RESPONSE_RE.findall(text):
                normalized = Path(target).as_posix()
                if normalized in ids:
                    edge_set.add((source, normalized, "serves"))

        if source.endswith((".html", ".js", ".css")):
            for match in WEB_REF_RE.finditer(text):
                value = match.group(1) or match.group(2) or ""
                local_file = _local_web_path(value)
                if local_file in ids and local_file != source:
                    edge_set.add((source, local_file, "loads"))
                route_path = value.split("?", 1)[0]
                # Exact routes are preferred; dynamic-looking calls also match their static prefix.
                for route, owners in routes.items():
                    if route_path == route or ("${" in value and route_path.startswith(route.split("{")[0])):
                        for owner in owners:
                            if owner != source:
                                edge_set.add((source, owner, "calls"))

    degree = {node_id: 0 for node_id in ids}
    for source, target, _ in edge_set:
        degree[source] += 1
        degree[target] += 1

    nodes = []
    for path in files:
        node_id = path.relative_to(PROJECT_ROOT).as_posix()
        folder = node_id.rsplit("/", 1)[0] if "/" in node_id else "root"
        insight = _file_insight(node_id, texts[node_id])
        owned_routes = [
            {"path": route["path"], "method": route["method"], "handler": route["handler"]}
            for route in route_records if route["owner"] == node_id
        ]
        nodes.append({
            "id": node_id,
            "name": path.name,
            "folder": folder,
            "kind": _kind(path),
            "size": path.stat().st_size,
            "connections": degree[node_id],
            "purpose": insight["purpose"],
            "symbols": insight["symbols"],
            "routes": owned_routes,
        })

    edges = [
        {"source": source, "target": target, "relation": relation}
        for source, target, relation in sorted(edge_set)
    ]
    return {
        "nodes": nodes,
        "edges": edges,
        "routes": route_records,
        "overview": {
            "summary": "A FastAPI learning app whose browser screens call Python API routers. Routers coordinate reusable services, AI/audio generation, caching, and Cloudflare R2 storage.",
            "layers": [
                {"name": "Browser screens", "folders": ["templates", "static"], "description": "What the learner sees and interacts with."},
                {"name": "API routers", "folders": ["routers"], "description": "Receive requests and choose what backend work happens."},
                {"name": "Services", "folders": ["services"], "description": "Reusable storage, AI, audio, cache, and deck logic."},
                {"name": "Data contracts", "folders": ["root"], "description": "Shared models, configuration, and application startup."},
            ],
        },
    }
