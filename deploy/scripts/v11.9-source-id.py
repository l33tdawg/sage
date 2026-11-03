#!/usr/bin/env python3
"""Print a collision-safe identity of the v11.9 fixture and evidence inputs.

Git is deliberately not consulted here. Docker does not honor Git's ignore
configuration, so a globally ignored or ``.git/info/exclude``-ignored Go file
can still enter ``COPY . .`` and affect a fixture binary. The source identity
instead walks the same small, deny-by-default source model as ``.dockerignore``.
It also binds the required scripts, topology, SQL, and workflow controls that
execute or interpret the composite release proof.
"""

import fnmatch
import hashlib
import os
from pathlib import Path
import stat
import struct


SOURCE_ID_DOMAIN = b"sage-v119-state-sync-source-v4\0"
REGULAR_KIND = b"F"
SYMLINK_KIND = b"L"
U32 = struct.Struct(">I")
U64 = struct.Struct(">Q")

# These inputs are copied by both fixture Dockerfiles. Keep this model small:
# adding a source root requires an intentional source-ID and .dockerignore
# change in the same patch.
SOURCE_FILES = ("go.mod", "go.sum")
SOURCE_DIRECTORIES = ("api", "cmd", "internal", "third_party", "web")

# Docker reads these even though the deny-by-default context does not make
# them available to COPY. They can change the resulting image and therefore
# belong to the identity as explicit build-control inputs.
BUILD_DOCKERFILES = ("deploy/Dockerfile.abci", "deploy/Dockerfile.node")
BUILD_CONTROL_FILES = (".dockerignore", *BUILD_DOCKERFILES)

# These files define how the frozen images and application sources are turned
# into release evidence. They are not Docker COPY inputs, but a change to one
# after the initial hash could otherwise alter topology, fault placement, or
# pass/fail interpretation without invalidating the final identity check.
EVIDENCE_CONTROL_FILES = (
    "deploy/scripts/v11.9-source-id.py",
    "deploy/scripts/v11.9-source-id-test.py",
    "deploy/scripts/run-v11.9-chaos.sh",
    "deploy/scripts/run-v11.9-state-sync.sh",
    "deploy/scripts/run-v11.9-multiprocess.sh",
    "deploy/init-testnet.sh",
    "deploy/docker-compose.yml",
    "deploy/docker-compose.test.yml",
    "deploy/docker-compose.v119-chaos.yml",
    "deploy/init.sql",
    "Makefile",
    ".github/workflows/v11.9-fault-gates.yml",
    ".github/workflows/ci.yml",
    ".github/workflows/release.yml",
)
CONTROL_FILES = (*BUILD_CONTROL_FILES, *EVIDENCE_CONTROL_FILES)

# A path is excluded when any component matches one of these rules. Rules are
# intentionally component-shaped so the Python walk and generated Docker
# patterns have the same meaning for both files and directory subtrees.
DENIED_COMPONENT_NAMES = (
    ".sage",
    ".codex",
    ".claude",
    ".git",
    ".idea",
    ".vscode",
    "data",
    "__pycache__",
    ".pytest_cache",
    "node_modules",
    ".next",
    "bin",
    "build",
    "dist",
    ".gocache",
    "test-results",
    "playwright-report",
    ".venv",
    "venv",
    ".eggs",
    ".mcp.json",
    ".env",
    "pending-import.json",
    "import-done.txt",
    "go.work",
    "go.work.sum",
    "MANIFEST",
    "KEYREGISTRY",
    "docker-compose.override.yml",
    "coverage.html",
    ".DS_Store",
    "Thumbs.db",
)
DENIED_COMPONENT_GLOBS = (
    "*.egg-info",
    "*.egg",
    ".mcpregistry_*",
    "*.key",
    ".env.*",
    "*.vlog",
    "*.sst",
    "*.py[cod]",
    "*.test",
    "*.prof",
    "*.out",
    "*~",
    "*.swp",
    "*.swo",
    "*.exe",
    "*.dll",
    "*.so",
    "*.dylib",
    "*.a",
    "*.o",
    "*.class",
    "*.jar",
)

# Browser vendor assets are production source embedded by web/embed.go. Other
# vendor components remain excluded. Later component-deny rules still apply
# inside this exception, so a .env or cache cannot be smuggled through it.
VENDOR_COMPONENT = "vendor"
VENDOR_SOURCE_PREFIX = ("web", "static", "js", "vendor")


def dockerignore_directives():
    """Return the only Docker context policy accepted by this source model."""

    directives = ["**"]
    directives.extend(f"!{path}" for path in SOURCE_FILES)
    for directory in SOURCE_DIRECTORIES:
        directives.extend((f"!{directory}/", f"!{directory}/**"))

    # Exclude all vendor components, then restore the one embedded asset tree.
    # Every remaining deny rule follows the exception and therefore still wins
    # within that tree.
    directives.extend(("**/vendor", "**/vendor/**"))
    vendor_path = "/".join(VENDOR_SOURCE_PREFIX)
    directives.extend((f"!{vendor_path}/", f"!{vendor_path}/**"))

    for name in DENIED_COMPONENT_NAMES:
        directives.extend((f"**/{name}", f"**/{name}/**"))
    for pattern in DENIED_COMPONENT_GLOBS:
        directives.extend((f"**/{pattern}", f"**/{pattern}/**"))
    return tuple(directives)


def _dockerignore_policy(root):
    path = os.path.join(root, ".dockerignore")
    info = os.lstat(path)
    if not stat.S_ISREG(info.st_mode):
        raise RuntimeError(".dockerignore must be a regular file")
    with open(path, "r", encoding="utf-8", newline="") as source:
        lines = source.read().splitlines()

    # Comments may document the policy, but every effective directive must be
    # byte-for-byte canonical. An extra negation could otherwise admit a file
    # that this walker does not know to hash.
    actual = tuple(line for line in lines if line and not line.startswith("#"))
    expected = dockerignore_directives()
    if actual != expected:
        raise RuntimeError(
            ".dockerignore does not match the v11.9 source-ID context policy"
        )


def _validate_control_files(root):
    for relative in (*SOURCE_FILES, *CONTROL_FILES):
        path = os.path.join(root, relative)
        try:
            info = os.lstat(path)
        except FileNotFoundError as error:
            raise RuntimeError(
                f"required v11.9 source/control input is missing: {relative}"
            ) from error
        if not stat.S_ISREG(info.st_mode):
            raise RuntimeError(
                f"v11.9 source/control input must be a regular file: {relative}"
            )

    # Dockerfile-specific ignore files override the root .dockerignore. Refuse
    # that second policy surface instead of risking a context/digest mismatch.
    for dockerfile in BUILD_DOCKERFILES:
        override = os.path.join(root, f"{dockerfile}.dockerignore")
        if os.path.lexists(override):
            raise RuntimeError(
                f"Dockerfile-specific ignore policy is not permitted: {dockerfile}.dockerignore"
            )
    _dockerignore_policy(root)


def _component_is_denied(component):
    if component in DENIED_COMPONENT_NAMES:
        return True
    return any(
        fnmatch.fnmatchcase(component, pattern)
        for pattern in DENIED_COMPONENT_GLOBS
    )


def _path_is_denied(parts):
    for component in parts:
        if component == VENDOR_COMPONENT and tuple(parts[:4]) == VENDOR_SOURCE_PREFIX:
            continue
        if component == VENDOR_COMPONENT or _component_is_denied(component):
            return True
    return False


def _walk_source_directory(root, relative_directory):
    paths = []
    pending = [relative_directory]
    while pending:
        relative = pending.pop()
        absolute = os.path.join(root, relative)
        try:
            entries = list(os.scandir(absolute))
        except (FileNotFoundError, NotADirectoryError) as error:
            raise RuntimeError(
                f"source directory changed while walking: {relative}"
            ) from error
        for entry in entries:
            child = os.path.join(relative, entry.name)
            parts = child.split(os.sep)
            if _path_is_denied(parts):
                continue
            try:
                info = entry.stat(follow_symlinks=False)
            except FileNotFoundError as error:
                raise RuntimeError(f"source path changed while walking: {child}") from error
            if stat.S_ISDIR(info.st_mode):
                pending.append(child)
            else:
                # source_identity rejects sockets, devices, and other special
                # objects. Symlinks are included without following them.
                paths.append(os.fsencode(child))
    return paths


def build_source_paths(root="."):
    """List every local input bound to the v11.9 composite release proof."""

    root = os.fspath(root)
    _validate_control_files(root)
    paths = [os.fsencode(path) for path in (*SOURCE_FILES, *CONTROL_FILES)]
    for directory in SOURCE_DIRECTORIES:
        absolute = os.path.join(root, directory)
        try:
            info = os.lstat(absolute)
        except FileNotFoundError:
            # An absent allowlisted directory adds nothing to Docker's context.
            continue
        if not stat.S_ISDIR(info.st_mode):
            raise RuntimeError(
                f"allowlisted source root must be a directory: {directory}"
            )
        paths.extend(_walk_source_directory(root, directory))
    return sorted(paths)


def _update_sized(digest, value):
    digest.update(U64.pack(len(value)))
    digest.update(value)


def _regular_identity(path, initial):
    content_digest = hashlib.sha256()
    observed_size = 0
    with open(path, "rb") as source:
        opened = os.fstat(source.fileno())
        if not stat.S_ISREG(opened.st_mode) or not os.path.samestat(initial, opened):
            raise RuntimeError(f"source file changed while opening: {path}")
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            observed_size += len(chunk)
            content_digest.update(chunk)
        completed = os.fstat(source.fileno())
    if (
        not os.path.samestat(initial, completed)
        or completed.st_size != initial.st_size
        or completed.st_mtime_ns != initial.st_mtime_ns
        or observed_size != completed.st_size
    ):
        raise RuntimeError(f"source file changed while hashing: {path}")
    return observed_size, content_digest.digest()


def _symlink_identity(path, initial):
    target = os.fsencode(os.readlink(path))
    completed = os.lstat(path)
    if (
        not stat.S_ISLNK(completed.st_mode)
        or not os.path.samestat(initial, completed)
        or completed.st_mtime_ns != initial.st_mtime_ns
    ):
        raise RuntimeError(f"source symlink changed while hashing: {path}")
    return len(target), hashlib.sha256(target).digest()


def source_identity(raw_paths, root="."):
    """Hash an exact filesystem tree with unambiguous, length-framed records."""

    paths = sorted(path for path in raw_paths if path)
    if len(paths) != len(set(paths)):
        raise ValueError("source identity received a duplicate path")

    digest = hashlib.sha256(SOURCE_ID_DOMAIN)
    digest.update(U64.pack(len(paths)))
    for raw in paths:
        if (
            not isinstance(raw, bytes)
            or b"\0" in raw
            or os.path.isabs(raw)
            or any(
                part in (b"", b".", b"..")
                for part in raw.split(os.fsencode(os.sep))
            )
        ):
            raise ValueError("source identity paths must be relative NUL-free bytes")
        relative_path = os.fsdecode(raw)
        path = os.path.join(root, relative_path)
        info = os.lstat(path)
        if stat.S_ISREG(info.st_mode):
            kind = REGULAR_KIND
            content_size, content_digest = _regular_identity(path, info)
        elif stat.S_ISLNK(info.st_mode):
            kind = SYMLINK_KIND
            content_size, content_digest = _symlink_identity(path, info)
        else:
            raise RuntimeError(f"source path is not a regular file or symlink: {relative_path}")

        # Each record is independently parseable: sized path, explicit object
        # kind, fixed-width mode/content length, and a fixed-width content hash.
        # File bytes can therefore never manufacture a second record boundary.
        digest.update(b"\x01")
        _update_sized(digest, raw)
        digest.update(kind)
        digest.update(U32.pack(stat.S_IMODE(info.st_mode)))
        digest.update(U64.pack(content_size))
        digest.update(content_digest)
    return digest.hexdigest()


def build_source_identity(root="."):
    """Hash a stable snapshot of the fixture and evidence-control input set."""

    before = build_source_paths(root)
    identity = source_identity(before, root)
    after = build_source_paths(root)
    if before != after:
        raise RuntimeError("fixture build-input set changed while hashing")
    return identity


def main():
    repository_root = Path(__file__).resolve().parents[2]
    print(build_source_identity(repository_root))


if __name__ == "__main__":
    main()
