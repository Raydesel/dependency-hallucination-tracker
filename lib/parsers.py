import json
import re


def parse_package_json(content: str) -> list[dict]:
    """Extract npm packages from a package.json file's content."""
    packages = []
    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        return packages

    for key in ("dependencies", "devDependencies"):
        deps = data.get(key, {})
        if not isinstance(deps, dict):
            continue
        for name, version in deps.items():
            packages.append({
                "package_name": name,
                "version_spec": version,
                "ecosystem": "npm",
            })
    return packages


# matches a leading package name: letters, digits, dots, hyphens, underscores
_PKG_NAME = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]*)")


def parse_requirements_txt(content: str) -> list[dict]:
    """Extract PyPI packages from a requirements.txt file's content."""
    packages = []
    for raw_line in content.splitlines():
        line = raw_line.strip()

        # skip blanks, comments, options, editable installs, and URLs
        if not line or line.startswith(("#", "-", "git+", "http")):
            continue

        # strip inline comments:  "requests==2.0  # pinned"  ->  "requests==2.0"
        line = line.split(" #")[0].strip()

        # strip environment markers:  "foo; python_version<'3.8'"  ->  "foo"
        line = line.split(";")[0].strip()

        match = _PKG_NAME.match(line)
        if not match:
            continue

        name = match.group(1)
        version_spec = line[len(name):].strip()  # whatever follows the name

        packages.append({
            "package_name": name,
            "version_spec": version_spec,
            "ecosystem": "pypi",
        })
    return packages


if __name__ == "__main__":
    sample_pkg_json = '''
    {
      "dependencies": { "express": "^4.18.0", "@scope/pkg": "1.0.0" },
      "devDependencies": { "jest": "^29.0.0" }
    }
    '''
    sample_reqs = """
    # this is a comment
    requests==2.31.0
    flask>=2.0
    pandas[excel]~=2.1
    -e git+https://github.com/x/y.git
    numpy ; python_version >= '3.9'
    """

    print("package.json →")
    for p in parse_package_json(sample_pkg_json):
        print("  ", p)

    print("requirements.txt →")
    for p in parse_requirements_txt(sample_reqs):
        print("  ", p)