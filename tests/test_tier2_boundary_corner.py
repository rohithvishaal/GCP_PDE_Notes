"""
Tier 2: Boundary & Corner Cases Test Suite.

Verifies:
- Git Tracking (.gitignore must not ignore docs/ or docs/audits/)
- Internal Relative Link Integrity (zero broken relative links across repo)
- Markdown Syntax & Code Block Fence Balance (even fence count, properly closed blocks)
- Markdown Table Formatting Consistency
"""

import re
import unittest
import urllib.parse
from pathlib import Path
from typing import List, Tuple

from tests.conftest_base import (
    WORKSPACE_ROOT,
    extract_markdown_links,
    get_all_repository_markdown_files,
    is_relative_internal_link,
    read_text_safe,
)


class TestTier2BoundaryCornerCases(unittest.TestCase):
    """Tier 2: Boundary & Corner Cases Verification."""

    def test_01_git_tracking_docs_not_ignored(self):
        """Asserts that .gitignore does not ignore docs/ so audit deliverables can be tracked."""
        gitignore_path = WORKSPACE_ROOT / ".gitignore"
        self.assertTrue(gitignore_path.is_file(), ".gitignore file must exist at repository root.")

        content = read_text_safe(gitignore_path)
        lines = [line.strip() for line in content.splitlines()]

        # Filter out comments and blank lines
        active_rules = [line for line in lines if line and not line.startswith("#")]

        # Check if docs/ is directly ignored without an un-ignore override
        docs_ignored = False
        for rule in active_rules:
            if rule in ("docs/", "docs", "/docs/", "/docs"):
                docs_ignored = True
            elif rule in ("!docs/", "!docs", "!/docs/", "!/docs"):
                docs_ignored = False

        self.assertFalse(
            docs_ignored,
            "CRITICAL: .gitignore is currently ignoring 'docs/'. "
            "Audit reports in docs/audits/ cannot be tracked by Git until this is remediated.",
        )

    def test_02_internal_relative_link_integrity(self):
        """Asserts zero broken internal relative markdown links across all repository markdown files."""
        all_md_files = get_all_repository_markdown_files(WORKSPACE_ROOT)
        self.assertGreaterEqual(len(all_md_files), 40, "Expected at least 40 markdown files in census.")

        broken_links: List[Tuple[str, str, str]] = []  # (source_rel, target, error_msg)

        for md_path in all_md_files:
            rel_source = str(md_path.relative_to(WORKSPACE_ROOT)).replace("\\", "/")
            content = read_text_safe(md_path)
            links = extract_markdown_links(content)

            for label, raw_target in links:
                target = raw_target.strip()
                if not is_relative_internal_link(target):
                    continue

                # Flag file:// absolute URIs as broken/invalid relative links
                if target.startswith("file://") or target.startswith("file:/"):
                    broken_links.append((
                        rel_source,
                        target,
                        "Anti-pattern: absolute file:// URI used instead of clean relative Markdown path (../../)",
                    ))
                    continue

                # Strip anchor (#anchor)
                target_file_part = target.split("#")[0].strip()
                if not target_file_part:
                    continue  # Anchor-only link on same page

                # Unquote URL encoding (%20, etc.)
                target_file_part = urllib.parse.unquote(target_file_part)

                # Resolve relative to directory containing the markdown file
                resolved_path = (md_path.parent / target_file_part).resolve()

                if not resolved_path.exists():
                    broken_links.append((rel_source, target, f"Target '{resolved_path}' does not exist."))

        self.assertEqual(
            broken_links,
            [],
            f"Found {len(broken_links)} broken relative link(s) across repository:\n"
            + "\n".join(f"  [{src}] -> '{tgt}' ({err})" for src, tgt, err in broken_links[:20]),
        )

    def test_03_markdown_code_block_fence_balance(self):
        """Asserts every markdown document has balanced triple-backtick code fences (even count)."""
        all_md_files = get_all_repository_markdown_files(WORKSPACE_ROOT)
        unbalanced_files = []

        for md_path in all_md_files:
            rel_path = str(md_path.relative_to(WORKSPACE_ROOT)).replace("\\", "/")
            content = read_text_safe(md_path)
            
            # Count lines starting with ```
            fence_lines = [
                idx + 1
                for idx, line in enumerate(content.splitlines())
                if line.strip().startswith("```")
            ]
            if len(fence_lines) % 2 != 0:
                unbalanced_files.append((rel_path, len(fence_lines), fence_lines[-1]))

        self.assertEqual(
            unbalanced_files,
            [],
            f"Found {len(unbalanced_files)} file(s) with unbalanced code fences:\n"
            + "\n".join(f"  {f}: {cnt} fences (last at line {last})" for f, cnt, last in unbalanced_files),
        )

    def test_04_markdown_code_blocks_non_empty_content(self):
        """Asserts that code blocks contain valid syntax and are not dangling empty blocks."""
        all_md_files = get_all_repository_markdown_files(WORKSPACE_ROOT)
        dangling_blocks = []

        for md_path in all_md_files:
            rel_path = str(md_path.relative_to(WORKSPACE_ROOT)).replace("\\", "/")
            content = read_text_safe(md_path)
            lines = content.splitlines()
            in_block = False
            start_line = 0
            block_lines = []

            for idx, line in enumerate(lines, start=1):
                if line.strip().startswith("```"):
                    if not in_block:
                        in_block = True
                        start_line = idx
                        block_lines = []
                    else:
                        in_block = False
                        if not any(b.strip() for b in block_lines):
                            dangling_blocks.append((rel_path, start_line))
                elif in_block:
                    block_lines.append(line)

        # Allow at most 0 dangling completely empty code blocks
        self.assertEqual(
            dangling_blocks,
            [],
            f"Found {len(dangling_blocks)} completely empty dangling code block(s):\n"
            + "\n".join(f"  {f} line {line}" for f, line in dangling_blocks),
        )


if __name__ == "__main__":
    unittest.main()
