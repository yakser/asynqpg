#!/usr/bin/env python3
"""Release script for asynqpg multi-module repository.

Usage:
    python3 scripts/release.py core v0.5.0
    python3 scripts/release.py ui v0.1.0
    python3 scripts/release.py ui v0.1.0 --core-version v0.4.0
"""

import argparse
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
MODULE = "github.com/yakser/asynqpg"

GREEN = "\033[32m"
RED = "\033[31m"
RESET = "\033[0m"


def info(msg: str) -> None:
    print(f"{GREEN}==>{RESET} {msg}")


def die(msg: str) -> None:
    print(f"{RED}error:{RESET} {msg}", file=sys.stderr)
    sys.exit(1)


def git(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(REPO_ROOT), *args],
        check=True,
        capture_output=True,
        text=True,
    )


def validate_version(version: str) -> None:
    if not re.fullmatch(r"v\d+\.\d+\.\d+", version):
        die(f"invalid version format: {version} (expected vX.Y.Z)")


def ensure_clean_tree() -> None:
    result = git("status", "--porcelain")
    if result.stdout.strip():
        die("working tree is not clean -- commit or stash changes first")


def ensure_on_master() -> None:
    result = git("rev-parse", "--abbrev-ref", "HEAD")
    branch = result.stdout.strip()
    if branch != "master":
        die(f"releases must be created from master (currently on {branch})")


def tag_exists(tag: str) -> bool:
    try:
        git("rev-parse", tag)
        return True
    except subprocess.CalledProcessError:
        return False


def latest_core_tag() -> str:
    result = git("tag", "--list", "v[0-9]*", "--sort=-v:refname")
    tags = result.stdout.strip().splitlines()
    return tags[0] if tags else ""


def release_core(version: str) -> None:
    tag = version

    if tag_exists(tag):
        die(f"tag {tag} already exists")

    info(f"Creating annotated tag {tag} for core module")
    git("tag", "-a", tag, "-m", f"Release {MODULE} {version}")

    info("Done. Push with:")
    print(f"  git push origin master --tags")


def release_ui(version: str, core_version: str) -> None:
    tag = f"ui/{version}"
    ui_gomod = REPO_ROOT / "ui" / "go.mod"

    if tag_exists(tag):
        die(f"tag {tag} already exists")

    # Determine core version to reference.
    if not core_version:
        core_version = latest_core_tag()
        if not core_version:
            die("no core tags found -- release core first or use --core-version")
        info(f"Using latest core tag: {core_version}")

    # Step 1: Remove replace directive, update require, tidy.
    info(f"Preparing ui/go.mod for release (core {core_version})")

    content = ui_gomod.read_text()

    # Remove the replace directive line.
    content = re.sub(
        r"\n?replace github\.com/yakser/asynqpg\s+=>\s+\.\./\n?",
        "\n",
        content,
    )

    # Update the require version for the core module (not the /ui subpath).
    content = re.sub(
        r"(github\.com/yakser/asynqpg) v\S+",
        rf"\1 {core_version}",
        content,
    )

    ui_gomod.write_text(content)

    # Tidy.
    subprocess.run(["go", "mod", "tidy"], check=True, cwd=REPO_ROOT / "ui")

    # Commit the go.mod/go.sum changes.
    git("add", "ui/go.mod", "ui/go.sum")
    git("commit", "-m", f"chore(ui): prepare release {version} (core {core_version})")

    # Step 2: Create annotated tag.
    info(f"Creating annotated tag {tag} for UI module")
    git("tag", "-a", tag, "-m", f"Release {MODULE}/ui {version}")

    # Step 3: Restore replace directive.
    info("Restoring replace directive in ui/go.mod")

    content = ui_gomod.read_text()

    # Restore pseudo-version for local dev.
    content = content.replace(
        f"github.com/yakser/asynqpg {core_version}",
        "github.com/yakser/asynqpg v0.0.0",
    )

    # Add replace directive back.
    content = content.rstrip() + "\n\nreplace github.com/yakser/asynqpg => ../\n"

    ui_gomod.write_text(content)

    subprocess.run(["go", "mod", "tidy"], check=True, cwd=REPO_ROOT / "ui")

    git("add", "ui/go.mod", "ui/go.sum")
    git("commit", "-m", f"chore(ui): restore replace directive after {version} release")

    info("Done. Push with:")
    print("  git push origin master --tags")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Release script for asynqpg multi-module repository.",
    )
    subparsers = parser.add_subparsers(dest="component")

    # core subcommand
    core_parser = subparsers.add_parser("core", help="Release core module")
    core_parser.add_argument("version", help="Version tag (e.g. v0.5.0)")

    # ui subcommand
    ui_parser = subparsers.add_parser("ui", help="Release UI module")
    ui_parser.add_argument("version", help="Version tag (e.g. v0.1.0)")
    ui_parser.add_argument(
        "--core-version",
        default="",
        help="Pin to a specific core version (default: latest core tag)",
    )

    # help subcommand
    subparsers.add_parser("help", help="Show this help message")

    args = parser.parse_args()

    if not args.component or args.component == "help":
        parser.print_help()
        sys.exit(0)

    validate_version(args.version)
    ensure_clean_tree()
    ensure_on_master()

    if args.component == "core":
        release_core(args.version)
    elif args.component == "ui":
        if args.core_version:
            validate_version(args.core_version)
        release_ui(args.version, args.core_version)


if __name__ == "__main__":
    main()
