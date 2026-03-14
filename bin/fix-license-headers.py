#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
"""
Validates and fixes ASF license headers across the TinkerPop repository.

Handles all comment styles found in the repo:
  - Java/Go/C# block comments  (* prefix inside /* ... */)
  - Double-slash                (// prefix)
  - Hash                        (# prefix)
  - AsciiDoc block comment      (content inside //// ... ////)
  - HTML/XML block comment      (content inside <!-- ... -->)
  - Batch files                 (:: prefix)
  - RST files                   (.. prefix)

Respects the rat-plugin exclusion list from the root pom.xml.

Usage:
    python3 bin/fix-license-headers.py           # report issues only
    python3 bin/fix-license-headers.py --fix     # report and fix issues
    python3 bin/fix-license-headers.py --verbose # show per-file details
"""

import os
import re
import sys
import fnmatch
from collections import Counter

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ---------------------------------------------------------------------------
# Canonical license text
# ---------------------------------------------------------------------------

# Lines of the license body with no comment prefix.
CANONICAL_LINES = [
    "Licensed to the Apache Software Foundation (ASF) under one",
    "or more contributor license agreements.  See the NOTICE file",
    "distributed with this work for additional information",
    "regarding copyright ownership.  The ASF licenses this file",
    "to you under the Apache License, Version 2.0 (the",
    '"License"); you may not use this file except in compliance',
    "with the License.  You may obtain a copy of the License at",
    "",
    "  http://www.apache.org/licenses/LICENSE-2.0",
    "",
    "Unless required by applicable law or agreed to in writing,",
    "software distributed under the License is distributed on an",
    '"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY',
    "KIND, either express or implied.  See the License for the",
    "specific language governing permissions and limitations",
    "under the License.",
]

# Full AsciiDoc block (content between //// delimiters, inclusive).
CANONICAL_ASCIIDOC = """\
////
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
////"""

# Full HTML/XML comment block (content between <!-- and -->, inclusive).
CANONICAL_HTML = """\
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->"""

# Phrases that mark the end of any Apache license block (stripped of prefix).
# The standard form ends with "under the License." on its own line; the
# paragraph-wrapped form ends with "limitations under the License."
LICENSE_END_PHRASES = ("under the License.", "limitations under the License.")

# ---------------------------------------------------------------------------
# Exclusion patterns (mirrors rat-plugin <excludes> in root pom.xml)
# ---------------------------------------------------------------------------

EXCLUDE_PATTERNS = [
    ".mailmap", ".asf.yaml", ".travis.yml", ".travis.*.sh", ".dockerignore",
    ".github/**",
    "**/.classpath", "**/.project", "**/.settings/**", "**/.idea/**",
    ".repository/**", "**/target/**",
    "data/*.txt",
    "**/bin/gremlin.sh", "gremlin-console/bin/gremlin.sh",
    "docs/static/**", "docs/original/**", "docs/site/home/css/**", "docs/site/home/js/**",
    "docs/gremlint/build/**", "docs/gremlint/public/CNAME",
    "**/AGENTS.md",
    "**/*.kryo", "**/*.gbin", "**/*.iml", "**/*.json", "**/*.xml",
    "**/*.ldjson", "**/*.graffle", "**/*.svg", "**/*.trx", "**/*.sln",
    "**/*.user", "**/*.csproj", "**/*.nuspec",
    "**/goal.txt",
    "**/src/main/resources/META-INF/services/**",
    "**/src/test/resources/mockito-extensions/**",
    "**/src/test/resources/META-INF/services/**",
    "**/src/test/resources/cucumber.properties",
    "**/src/test/resources/incorrect-traversals.txt",
    "**/src/test/resources/org/apache/tinkerpop/gremlin/console/groovy/plugin/script-customizer-*.groovy",
    "**/src/test/resources/org/apache/tinkerpop/gremlin/jsr223/script-customizer-*.groovy",
    "**/src/test/resources/org/apache/tinkerpop/gremlin/console/jsr223/script-customizer-*.groovy",
    "**/src/main/resources/org/apache/tinkerpop/gremlin/structure/io/script/*.txt",
    "**/src/main/ext/**", "**/src/main/static/**",
    "**/_bsp/**",
    "DEPENDENCIES", "**/.glv",
    "**/Debug/**", "**/Release/**", "**/obj/**",
    "**/.vs/**", "**/NuGet.Config", "**/BenchmarkDotNet.Artifacts/**",
    "**/.nvmrc", "**/.yarnrc.yml", "**/yarn.lock",
    "**/node/**", "**/node_modules/**", "**/npm-debug.log",
    "**/build/**", "**/doc/**", "**/lib/**",
    "**/.env", "**/.prettierrc", "**/_site/**",
    "**/.pytest_cache/**", "**/venv/**", "**/.venv/**", "**/.eggs/**",
    "**/gremlinpython.egg-info/**", "**/docfx/**",
    "**/go.sum", "**/coverage.out", "**/gremlinconsoletest.egg-info/**",
]

# Directories never descended into (faster than pattern matching every file).
PRUNE_DIRS = {'.git', 'target', 'node_modules', 'build', 'venv', '.venv',
              '.eggs', 'doc', 'lib', 'Debug', 'Release', 'obj', 'docfx',
              '_site', '__pycache__', '.pytest_cache', 'BenchmarkDotNet.Artifacts'}

# ---------------------------------------------------------------------------
# Pattern matching
# ---------------------------------------------------------------------------

def matches_exclude_pattern(rel_path, patterns):
    rel_path = rel_path.replace(os.sep, "/")
    for pattern in patterns:
        pattern = pattern.replace(os.sep, "/")
        if "/" not in pattern:
            if fnmatch.fnmatch(os.path.basename(rel_path), pattern):
                return True
        elif "**" in pattern:
            if pattern.startswith("**/"):
                inner = pattern[3:]
                parts = rel_path.split("/")
                for i in range(len(parts)):
                    if fnmatch.fnmatch("/".join(parts[i:]), inner):
                        return True
            if fnmatch.fnmatch(rel_path, pattern):
                return True
        else:
            if fnmatch.fnmatch(rel_path, pattern) or fnmatch.fnmatch(os.path.basename(rel_path), pattern):
                return True
    return False

# ---------------------------------------------------------------------------
# AsciiDoc handling  (//// ... ////)
# ---------------------------------------------------------------------------

def process_asciidoc(filepath, fix):
    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
        content = f.read()

    if 'Licensed to the Apache Software Foundation' not in content:
        return 'no_license', []

    lines = content.splitlines(keepends=True)

    first = next((i for i, l in enumerate(lines) if l.rstrip('\r\n') == '////'), None)
    if first is None:
        return 'unparseable', ['no opening //// delimiter found']

    second = next((i for i in range(first + 1, len(lines)) if lines[i].rstrip('\r\n') == '////'), None)
    if second is None:
        return 'unparseable', ['no closing //// delimiter found']

    body = '\n'.join(l.rstrip('\r\n') for l in lines[first:second + 1])
    if body == CANONICAL_ASCIIDOC:
        return 'ok', []

    if fix:
        new_content = CANONICAL_ASCIIDOC + '\n' + ''.join(lines[second + 1:])
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(new_content)
        return 'fixed', ['license block replaced with canonical form']

    return 'has_issues', ['license block does not match canonical form']

# ---------------------------------------------------------------------------
# HTML/XML comment handling  (<!-- ... -->)
# ---------------------------------------------------------------------------

def process_html_comment(filepath, fix, lines, open_idx):
    """Handle a <!-- --> comment block starting at open_idx."""
    close_idx = next(
        (i for i in range(open_idx + 1, len(lines)) if lines[i].rstrip('\r\n') == '-->'),
        None
    )
    if close_idx is None:
        return 'unparseable', ['no closing --> delimiter found']

    body = '\n'.join(l.rstrip('\r\n') for l in lines[open_idx:close_idx + 1])
    if body == CANONICAL_HTML:
        return 'ok', []

    if fix:
        new_content = (
            ''.join(lines[:open_idx])
            + CANONICAL_HTML + '\n'
            + ''.join(lines[close_idx + 1:])
        )
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(new_content)
        return 'fixed', ['html/xml comment block replaced with canonical form']

    return 'has_issues', ['html/xml comment block does not match canonical form']

# ---------------------------------------------------------------------------
# Generic comment-style handling
# ---------------------------------------------------------------------------

def detect_comment_style(lines, start_idx):
    """
    Detect the comment style from the line at start_idx.

    Returns (style, base_prefix).  Styles:
      block_star   — ' * ' prefix  (Java/JS/C# block comments)
      double_slash — '// ' prefix
      hash         — '# ' prefix
      double_colon — ':: ' prefix  (batch files)
      double_dot   — '.. ' prefix  (RST)
      plain        — no prefix
    """
    line = lines[start_idx].rstrip('\n').rstrip('\r')

    m = re.match(r'^(\s*\*\s+)', line)
    if m:
        return 'block_star', m.group(1)

    m = re.match(r'^(//\s*)', line)
    if m:
        return 'double_slash', m.group(1)

    m = re.match(r'^(::\s*)', line)
    if m:
        return 'double_colon', m.group(1)

    m = re.match(r'^(\.\.\s+)', line)
    if m:
        return 'double_dot', m.group(1)

    m = re.match(r'^(#\s*)', line)
    if m:
        first_prefix = m.group(1)
        prefixes_seen = [first_prefix]
        for j in range(start_idx + 1, min(start_idx + 10, len(lines))):
            next_line = lines[j].rstrip('\n').rstrip('\r')
            if next_line.rstrip() == '#':
                continue
            m2 = re.match(r'^(#\s*)', next_line)
            if m2 and next_line.strip():
                prefixes_seen.append(m2.group(1))
        prefix_counter = Counter(prefixes_seen)
        return 'hash', prefix_counter.most_common(1)[0][0]

    m = re.match(r'^(\s+)', line)
    if m and line.strip().startswith('Licensed'):
        return 'space_indent', m.group(1)

    return 'plain', ''


def get_line_content(line, style, base_prefix):
    """Strip the comment prefix and return the bare content of a line."""
    raw = line.rstrip('\n').rstrip('\r')

    if style == 'block_star':
        m = re.match(r'^(\s*\*)(.*)', raw)
        if m:
            full = m.group(1) + m.group(2)
            return full[len(base_prefix):].rstrip() if len(full) >= len(base_prefix) else ""
        return raw.rstrip()

    if style in ('double_slash', 'hash'):
        char = '//' if style == 'double_slash' else '#'
        m = re.match(r'^(' + re.escape(char) + r')(.*)', raw)
        if m:
            full = char + m.group(2)
            return full[len(base_prefix):].rstrip() if len(full) >= len(base_prefix) else ""
        return raw.strip()

    if style == 'double_colon':
        if raw.rstrip() == '::':
            return ""
        m = re.match(r'^(::\s*)(.*)', raw)
        if m:
            full = '::' + raw[2:]
            return full[len(base_prefix):].rstrip() if len(full) >= len(base_prefix) else m.group(2).rstrip()
        return raw.strip()

    if style == 'double_dot':
        if raw.rstrip() == '..':
            return ""
        if not raw.strip():
            return ""
        full = '..' + raw[2:]
        return full[len(base_prefix):].rstrip() if len(full) >= len(base_prefix) else raw.strip()

    if style == 'space_indent':
        if not raw.strip():
            return ""
        return raw[len(base_prefix):].rstrip() if len(raw) >= len(base_prefix) else raw.rstrip()

    return raw.rstrip()  # plain


def find_license_block(lines, style, base_prefix):
    """
    Locate start and end line indices of the license block.

    Accepts both the standard ending ('under the License.') and the
    paragraph-wrapped ending ('limitations under the License.').
    """
    start_idx = next(
        (i for i, l in enumerate(lines) if 'Licensed to the Apache Software Foundation' in l),
        None
    )
    if start_idx is None:
        return None

    end_idx = None
    for i in range(start_idx, min(start_idx + 55, len(lines))):
        content = get_line_content(lines[i], style, base_prefix).rstrip()
        if content in LICENSE_END_PHRASES:
            end_idx = i
            break

    if end_idx is None:
        return None

    num_lines = end_idx - start_idx + 1
    if not (14 <= num_lines <= 22):
        return None

    return start_idx, end_idx


def reconstruct_license_lines(style, base_prefix):
    """Build the corrected license block lines (with newlines)."""
    new_lines = []
    for canonical in CANONICAL_LINES:
        if canonical == "":
            if style == 'block_star':
                blank = re.match(r'^(\s*\*)', base_prefix)
                new_lines.append((blank.group(1) if blank else "") + "\n")
            elif style == 'double_slash':
                new_lines.append("//\n")
            elif style == 'hash':
                new_lines.append("#\n")
            elif style == 'double_colon':
                new_lines.append("::\n")
            elif style == 'double_dot':
                new_lines.append("..\n")
            else:
                new_lines.append("\n")
        else:
            new_lines.append((base_prefix if style != 'plain' else "") + canonical + "\n")
    return new_lines


def process_generic(filepath, fix):
    try:
        with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read()
    except Exception as e:
        return 'error', [str(e)]

    if 'Licensed to the Apache Software Foundation' not in content:
        return 'no_license', []

    lines = content.splitlines(keepends=True)

    licensed_idx = next(
        (i for i, l in enumerate(lines) if 'Licensed to the Apache Software Foundation' in l),
        None
    )
    if licensed_idx is None:
        return 'no_license', []

    # Check for HTML/XML comment block (<!-- on the line before Licensed)
    if licensed_idx > 0 and lines[licensed_idx - 1].rstrip('\r\n') == '<!--':
        return process_html_comment(filepath, fix, lines, licensed_idx - 1)

    style, base_prefix = detect_comment_style(lines, licensed_idx)
    result = find_license_block(lines, style, base_prefix)
    if result is None:
        return 'unparseable', ['could not locate complete license block']

    start_idx, end_idx = result
    extracted = [get_line_content(lines[i], style, base_prefix) for i in range(start_idx, end_idx + 1)]

    mismatches = []
    if len(extracted) != len(CANONICAL_LINES):
        mismatches.append(f"line count: got {len(extracted)}, expected {len(CANONICAL_LINES)}")
    for i in range(min(len(extracted), len(CANONICAL_LINES))):
        if extracted[i] != CANONICAL_LINES[i]:
            mismatches.append(f"line {i}: got {repr(extracted[i])}, expected {repr(CANONICAL_LINES[i])}")

    if not mismatches:
        return 'ok', []

    if fix:
        try:
            new_lines = (
                lines[:start_idx]
                + reconstruct_license_lines(style, base_prefix)
                + lines[end_idx + 1:]
            )
            with open(filepath, 'w', encoding='utf-8', errors='replace') as f:
                f.writelines(new_lines)
            return 'fixed', mismatches
        except Exception as e:
            return 'error', [f"fix failed: {e}"]

    return 'has_issues', mismatches

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    fix_mode = '--fix' in sys.argv
    verbose_mode = '--verbose' in sys.argv

    print(f"Repository: {REPO_ROOT}")
    print(f"Mode:       {'fix' if fix_mode else 'check only'}")
    print()

    stats = Counter()
    problems = {}    # rel_path -> list of issue strings
    unparseable = []

    for dirpath, dirnames, filenames in os.walk(REPO_ROOT):
        dirnames[:] = sorted(d for d in dirnames if d not in PRUNE_DIRS)

        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            rel_path = os.path.relpath(filepath, REPO_ROOT).replace(os.sep, '/')

            if matches_exclude_pattern(rel_path, EXCLUDE_PATTERNS):
                continue

            if filename.endswith('.asciidoc'):
                status, issues = process_asciidoc(filepath, fix_mode)
            else:
                status, issues = process_generic(filepath, fix_mode)

            stats[status] += 1

            if status == 'has_issues':
                problems[rel_path] = issues
            elif status == 'unparseable':
                unparseable.append((rel_path, issues))
            elif status in ('fixed', 'ok') and verbose_mode:
                print(f"{'FIXED' if status == 'fixed' else 'OK   '} {rel_path}")

    total = sum(stats.values())
    print(f"Files scanned:    {total}")
    print(f"  No license:     {stats['no_license']}")
    print(f"  OK:             {stats['ok']}")
    print(f"  Fixed:          {stats['fixed']}")
    print(f"  Has issues:     {stats['has_issues']}")
    print(f"  Unparseable:    {stats['unparseable']}")
    print(f"  Errors:         {stats['error']}")

    if problems:
        print(f"\n=== FILES WITH LICENSE ISSUES ({len(problems)}) ===")
        for rel_path, issues in sorted(problems.items()):
            print(f"\n  {rel_path}:")
            for issue in issues[:5]:
                print(f"    {issue}")

    if unparseable:
        print(f"\n=== UNPARSEABLE LICENSE BLOCKS ({len(unparseable)}) ===")
        for rel_path, issues in unparseable:
            print(f"  {rel_path}: {issues[0]}")

    return len(problems)


if __name__ == '__main__':
    sys.exit(main())
