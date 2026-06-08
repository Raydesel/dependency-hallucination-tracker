import httpx


# A small set of very popular packages used for typosquat detection. These are
# the names attackers most often impersonate, and the names LLMs most often
# mangle when hallucinating.
POPULAR_NPM = {
    "react", "lodash", "express", "axios", "chalk", "commander", "react-dom",
    "webpack", "moment", "vue", "typescript", "next", "eslint", "jest",
    "dotenv", "uuid", "request", "debug", "async", "bluebird",
}

POPULAR_PYPI = {
    "requests", "numpy", "pandas", "flask", "django", "scipy", "pytest",
    "boto3", "urllib3", "setuptools", "pillow", "matplotlib", "tensorflow",
    "torch", "scikit-learn", "beautifulsoup4", "sqlalchemy", "click", "tqdm",
    "pydantic",
}

# Python standard-library top-level modules. These are absent from PyPI because
# they ship with Python — being "not in the registry" is expected, not a
# hallucination.
PY_STDLIB = {
    "os", "sys", "re", "json", "math", "time", "datetime", "subprocess",
    "threading", "logging", "collections", "itertools", "functools", "typing",
    "pathlib", "io", "socket", "struct", "hashlib", "base64", "random",
    "sqlite3", "urllib", "http", "asyncio", "unittest", "argparse", "csv",
    "shutil", "glob", "tempfile", "copy", "enum", "abc", "dataclasses",
    "contextlib", "traceback", "warnings", "inspect", "pickle", "queue",
    "signal", "uuid", "secrets", "string", "textwrap", "decimal", "operator",
    "platform", "shlex", "gzip", "zipfile", "xml", "html", "ssl", "email",
}


def benign_nonexistent(name: str, ecosystem: str) -> str | None:
    """Classify a not-in-registry name that is NOT a hallucination.

    Returns a short reason code, or None if the absence is genuinely suspicious
    (i.e. a likely hallucinated package). Only meaningful for names already known
    to be absent from the registry.
    """
    # dotted names like "urllib.parse" or "dateutil.relativedelta" are Python
    # import paths, not installable package names
    if "." in name:
        return "IMPORT_PATH"
    if ecosystem == "pypi" and name in PY_STDLIB:
        return "STDLIB"
    # monorepo-internal workspace scopes that are never published publicly
    if ecosystem == "npm" and name.startswith("@repo/"):
        return "INTERNAL"
    return None


def levenshtein(a: str, b: str) -> int:
    """Classic edit distance between two strings."""
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)

    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        curr = [i]
        for j, cb in enumerate(b, 1):
            cost = 0 if ca == cb else 1
            curr.append(min(
                prev[j] + 1,       # deletion
                curr[j - 1] + 1,   # insertion
                prev[j - 1] + cost # substitution
            ))
        prev = curr
    return prev[-1]


MIN_TARGET_LEN = 5   # ignore very short popular names (vue, zod, next) — too noisy


def typosquat_target(name: str, ecosystem: str) -> str | None:
    """Return the popular package this name is suspiciously close to, if any.

    A small edit distance from a popular name (while not being that name) is the
    classic typosquat signature, e.g. 'reqeusts' vs 'requests'. To keep the
    signal trustworthy we:
      - ignore popular targets shorter than MIN_TARGET_LEN (short names collide
        with too many legitimate packages, e.g. 'sympy' vs 'numpy')
      - allow distance 2 only for longer targets (>= 7 chars); otherwise the
        match must be an exact single-character edit
    """
    popular = POPULAR_NPM if ecosystem == "npm" else POPULAR_PYPI
    if name in popular:
        return None

    best_target = None
    best_dist = 99
    for target in popular:
        if len(target) < MIN_TARGET_LEN:
            continue
        dist = levenshtein(name, target)
        max_allowed = 2 if len(target) >= 7 else 1
        if 1 <= dist <= max_allowed and dist < best_dist:
            best_target, best_dist = target, dist
    return best_target


def check_npm(client: httpx.Client, name: str) -> dict:
    """Look up a package on the npm registry."""
    resp = client.get(f"https://registry.npmjs.org/{name}")
    if resp.status_code == 404:
        return {"exists": False, "first_published": None, "latest_version": None}
    resp.raise_for_status()
    data = resp.json()

    # the "time" object maps versions -> timestamps; "created" is first publish
    time_info = data.get("time", {})
    first_published = time_info.get("created")
    latest_version = data.get("dist-tags", {}).get("latest")

    return {
        "exists": True,
        "first_published": first_published,
        "latest_version": latest_version,
    }


def check_pypi(client: httpx.Client, name: str) -> dict:
    """Look up a package on PyPI."""
    resp = client.get(f"https://pypi.org/pypi/{name}/json")
    if resp.status_code == 404:
        return {"exists": False, "first_published": None, "latest_version": None}
    resp.raise_for_status()
    data = resp.json()

    info = data.get("info", {})
    latest_version = info.get("version")

    # find the earliest upload across all releases = first published
    first_published = None
    earliest = None
    for files in data.get("releases", {}).values():
        for f in files:
            ts = f.get("upload_time_iso_8601")
            if ts and (earliest is None or ts < earliest):
                earliest = ts
    first_published = earliest

    return {
        "exists": True,
        "first_published": first_published,
        "latest_version": latest_version,
    }


def check_registry(client: httpx.Client, name: str, ecosystem: str) -> dict:
    """Dispatch to the correct registry based on ecosystem."""
    if ecosystem == "npm":
        return check_npm(client, name)
    elif ecosystem == "pypi":
        return check_pypi(client, name)
    return {"exists": None, "first_published": None, "latest_version": None}
