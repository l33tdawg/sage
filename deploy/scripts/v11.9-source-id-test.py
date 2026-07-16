#!/usr/bin/env python3
"""Regression tests for the v11.9 exact fixture-source identity."""

import hashlib
import importlib.util
import os
from pathlib import Path
import stat
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("v11.9-source-id.py")
REPOSITORY_ROOT = SCRIPT.parents[2]
SPEC = importlib.util.spec_from_file_location("v119_source_id", SCRIPT)
SOURCE_ID = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SOURCE_ID)


def old_ambiguous_identity(paths, root):
    digest = hashlib.sha256(b"sage-v119-state-sync-source-v1\0")
    for raw in sorted(paths):
        path = Path(root, os.fsdecode(raw))
        info = os.lstat(path)
        digest.update(raw + b"\0" + format(stat.S_IMODE(info.st_mode), "04o").encode() + b"\0")
        if stat.S_ISLNK(info.st_mode):
            digest.update(os.fsencode(os.readlink(path)))
        else:
            digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def write_fixture_tree(root):
    root = Path(root)
    (root / "internal").mkdir()
    (root / "go.mod").write_text("module example.test/source-id\n", encoding="utf-8")
    (root / "go.sum").write_text("", encoding="utf-8")
    for relative in SOURCE_ID.CONTROL_FILES:
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"control:{relative}\n", encoding="utf-8")
    directives = "\n".join(SOURCE_ID.dockerignore_directives())
    (root / ".dockerignore").write_text(
        f"# generated test policy\n{directives}\n", encoding="utf-8"
    )


class SourceIdentityTests(unittest.TestCase):
    def test_file_bytes_cannot_manufacture_another_tree_record(self):
        with (
            tempfile.TemporaryDirectory() as single_root,
            tempfile.TemporaryDirectory() as split_root,
        ):
            Path(single_root, "a").write_bytes(b"X\x00b\x000644\x00Y")
            Path(split_root, "a").write_bytes(b"X")
            Path(split_root, "b").write_bytes(b"Y")
            for root, names in ((single_root, ("a",)), (split_root, ("a", "b"))):
                for name in names:
                    os.chmod(Path(root, name), 0o644)

            self.assertEqual(
                old_ambiguous_identity([b"a"], single_root),
                old_ambiguous_identity([b"a", b"b"], split_root),
                "the regression fixture must collide under the retired v1 framing",
            )
            self.assertNotEqual(
                SOURCE_ID.source_identity([b"a"], single_root),
                SOURCE_ID.source_identity([b"a", b"b"], split_root),
            )

    def test_object_kind_is_part_of_the_identity(self):
        with (
            tempfile.TemporaryDirectory() as regular_root,
            tempfile.TemporaryDirectory() as link_root,
        ):
            regular = Path(regular_root, "entry")
            regular.write_bytes(b"target")
            link = Path(link_root, "entry")
            os.symlink("target", link)
            os.chmod(regular, stat.S_IMODE(os.lstat(link).st_mode))

            self.assertEqual(
                old_ambiguous_identity([b"entry"], regular_root),
                old_ambiguous_identity([b"entry"], link_root),
                "the regression fixture must collide when object kind is omitted",
            )
            self.assertNotEqual(
                SOURCE_ID.source_identity([b"entry"], regular_root),
                SOURCE_ID.source_identity([b"entry"], link_root),
            )

    def test_order_is_canonical_and_content_changes_identity(self):
        with tempfile.TemporaryDirectory() as root:
            Path(root, "a").write_bytes(b"alpha")
            Path(root, "b").write_bytes(b"beta")
            forward = SOURCE_ID.source_identity([b"a", b"b"], root)
            reverse = SOURCE_ID.source_identity([b"b", b"a"], root)
            self.assertEqual(forward, reverse)
            self.assertRegex(forward, r"^[0-9a-f]{64}$")

            Path(root, "b").write_bytes(b"changed")
            self.assertNotEqual(forward, SOURCE_ID.source_identity([b"a", b"b"], root))

    def test_repository_dockerignore_is_the_canonical_shared_policy(self):
        SOURCE_ID.build_source_paths(REPOSITORY_ROOT)

    def test_git_ignored_go_source_is_hashed(self):
        with tempfile.TemporaryDirectory() as root:
            write_fixture_tree(root)
            ignored = Path(root, "internal", "ignored.go")
            ignored.write_text("package internal\nconst ignored = 1\n", encoding="utf-8")

            subprocess.run(["git", "init", "-q"], cwd=root, check=True)
            Path(root, ".git", "info", "exclude").write_text(
                "/internal/ignored.go\n", encoding="utf-8"
            )
            git_paths = subprocess.check_output(
                ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
                cwd=root,
            ).split(b"\0")
            self.assertNotIn(b"internal/ignored.go", git_paths)
            self.assertIn(b"internal/ignored.go", SOURCE_ID.build_source_paths(root))

            before = SOURCE_ID.build_source_identity(root)
            ignored.write_text("package internal\nconst ignored = 2\n", encoding="utf-8")
            self.assertNotEqual(before, SOURCE_ID.build_source_identity(root))

    def test_gate_control_change_alters_identity(self):
        with tempfile.TemporaryDirectory() as root:
            write_fixture_tree(root)
            paths = SOURCE_ID.build_source_paths(root)
            for relative in SOURCE_ID.EVIDENCE_CONTROL_FILES:
                self.assertIn(os.fsencode(relative), paths)

            before = SOURCE_ID.build_source_identity(root)
            control = Path(root, "deploy", "scripts", "run-v11.9-state-sync.sh")
            control.write_text("changed gate behavior\n", encoding="utf-8")
            self.assertNotEqual(before, SOURCE_ID.build_source_identity(root))

    def test_global_git_exclude_cannot_hide_source(self):
        with tempfile.TemporaryDirectory() as root:
            write_fixture_tree(root)
            ignored = Path(root, "internal", "global.go")
            ignored.write_text("package internal\nconst global = true\n", encoding="utf-8")
            excludes = Path(root, "global-excludes")
            excludes.write_text("/internal/global.go\n", encoding="utf-8")

            subprocess.run(["git", "init", "-q"], cwd=root, check=True)
            git_paths = subprocess.check_output(
                [
                    "git",
                    "-c",
                    f"core.excludesFile={excludes}",
                    "ls-files",
                    "--cached",
                    "--others",
                    "--exclude-standard",
                    "-z",
                ],
                cwd=root,
            ).split(b"\0")
            self.assertNotIn(b"internal/global.go", git_paths)
            self.assertIn(b"internal/global.go", SOURCE_ID.build_source_paths(root))

    def test_explicitly_excluded_secrets_and_caches_are_not_hashed(self):
        with tempfile.TemporaryDirectory() as root:
            write_fixture_tree(root)
            secret = Path(root, "internal", ".env")
            secret.write_text("TOKEN=first\n", encoding="utf-8")
            cache = Path(root, "internal", "node_modules", "poison.go")
            cache.parent.mkdir()
            cache.write_text("package poison\n", encoding="utf-8")
            venv = Path(root, "internal", ".venv", "lib", "poison.go")
            venv.parent.mkdir(parents=True)
            venv.write_text("package poison\n", encoding="utf-8")
            excluded_key = Path(root, "internal", "test.key")
            excluded_key.write_text("private\n", encoding="utf-8")
            binary = Path(root, "internal", "stale.so")
            binary.write_bytes(b"compiled artifact")

            paths = SOURCE_ID.build_source_paths(root)
            self.assertNotIn(b"internal/.env", paths)
            self.assertNotIn(b"internal/node_modules/poison.go", paths)
            self.assertNotIn(b"internal/.venv/lib/poison.go", paths)
            self.assertNotIn(b"internal/test.key", paths)
            self.assertNotIn(b"internal/stale.so", paths)
            before = SOURCE_ID.build_source_identity(root)

            secret.write_text("TOKEN=second\n", encoding="utf-8")
            cache.write_text("package changed\n", encoding="utf-8")
            venv.write_text("package changed\n", encoding="utf-8")
            excluded_key.write_text("different\n", encoding="utf-8")
            binary.write_bytes(b"different artifact")
            self.assertEqual(before, SOURCE_ID.build_source_identity(root))

    def test_embedded_browser_vendor_assets_remain_hashed(self):
        with tempfile.TemporaryDirectory() as root:
            write_fixture_tree(root)
            browser_vendor = Path(root, "web", "static", "js", "vendor", "asset.js")
            browser_vendor.parent.mkdir(parents=True)
            browser_vendor.write_text("first", encoding="utf-8")
            intentional_asset = Path(root, "web", "static", "assets", "brain.obj")
            intentional_asset.parent.mkdir(parents=True)
            intentional_asset.write_text("intentional 3d asset", encoding="utf-8")
            other_vendor = Path(root, "internal", "vendor", "ignored.go")
            other_vendor.parent.mkdir()
            other_vendor.write_text("package ignored\n", encoding="utf-8")

            paths = SOURCE_ID.build_source_paths(root)
            self.assertIn(b"web/static/js/vendor/asset.js", paths)
            self.assertIn(b"web/static/assets/brain.obj", paths)
            self.assertNotIn(b"internal/vendor/ignored.go", paths)
            before = SOURCE_ID.build_source_identity(root)
            browser_vendor.write_text("second", encoding="utf-8")
            self.assertNotEqual(before, SOURCE_ID.build_source_identity(root))

    def test_policy_drift_and_per_dockerfile_overrides_fail_closed(self):
        with tempfile.TemporaryDirectory() as root:
            write_fixture_tree(root)
            dockerignore = Path(root, ".dockerignore")
            dockerignore.write_text(
                dockerignore.read_text(encoding="utf-8") + "!unmodeled-source/**\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(RuntimeError, "does not match"):
                SOURCE_ID.build_source_paths(root)

        with tempfile.TemporaryDirectory() as root:
            write_fixture_tree(root)
            Path(root, "deploy", "Dockerfile.abci.dockerignore").write_text(
                "**\n", encoding="utf-8"
            )
            with self.assertRaisesRegex(RuntimeError, "specific ignore policy"):
                SOURCE_ID.build_source_paths(root)

    def test_missing_or_nonregular_control_file_fails_closed(self):
        with tempfile.TemporaryDirectory() as root:
            write_fixture_tree(root)
            Path(root, "deploy", "init.sql").unlink()
            with self.assertRaisesRegex(RuntimeError, "required v11.9 source/control input"):
                SOURCE_ID.build_source_paths(root)

        with tempfile.TemporaryDirectory() as root:
            write_fixture_tree(root)
            workflow = Path(root, ".github", "workflows", "v11.9-fault-gates.yml")
            workflow.unlink()
            workflow.mkdir()
            with self.assertRaisesRegex(RuntimeError, "must be a regular file"):
                SOURCE_ID.build_source_paths(root)


if __name__ == "__main__":
    unittest.main()
