#!/usr/bin/env python3
"""Verify the version of the Package with the version in Git tag."""

import os
import re
from pathlib import Path

repo_dir = Path(__file__).parent.parent.parent

path_of_init_file = Path(repo_dir / "fivetran_provider_async" / "__init__.py")
version_file = path_of_init_file.read_text()
git_ref = os.getenv("GITHUB_REF", "")
git_tag = git_ref.replace("refs/tags/", "")
version = re.findall('__version__ = "(.*)"', version_file)[0]

if git_tag is not None:
    if version != git_tag:
        raise SystemExit(f"The version in {path_of_init_file} ({version}) does not match the Git Tag ({git_tag}).")
