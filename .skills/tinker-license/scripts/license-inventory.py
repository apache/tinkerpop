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

"""Inventory what TinkerPop's binary artifacts bundle and cross-check it against their LICENSE and NOTICE.

Usage:
  license-inventory.py [--strict] [--skip-build] [--only <artifact-key>[,<artifact-key>...]]

Run from the repository root. Artifact keys: shaded, driver, console-uber, server-zip, console-zip,
source, glv. The glv key checks the language-variant packages' own LICENSE and NOTICE.

Every run reviews a fresh build. The script reads the version from the root pom.xml, whatever it
is (a SNAPSHOT before a release, the release version during one), then runs
  mvn clean package -DskipTests -pl gremlin-driver,gremlin-console,gremlin-server -am
and checks the artifacts of exactly that version. It never uses what target/ held before the run.
The build uses package, never install, so ~/.m2 is left as it was. Its output goes to
target/license-inventory-build.log. --only source and --only glv skip the build, since neither
needs one.

--skip-build reuses the artifacts already in target/ for the pom version and prints each one's
build time. If any artifact it needs is missing, it checks nothing and exits 2 instead, so the
caller can offer to build.

Exit codes: 0 checks ran (and, with --strict, passed); 1 a mechanical check failed under --strict,
or the build failed; 2 --skip-build found artifacts missing and nothing was checked.

The script reports facts only. It decides nothing about whether a license needs an entry; the
SKILL.md procedure does that. With --strict it exits 1 when a mechanical check fails:
a packed LICENSE/NOTICE differs from its source, a referenced licenses/ file is missing or a
packed one is unreferenced, or a LICENSE entry names an artifact or version that is not bundled.
"""

import argparse
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
from xml.etree import ElementTree

TP_GROUP = "org.apache.tinkerpop"
ASF_GROUP_PREFIXES = ("org.apache.", "commons-")
BOILERPLATE = re.compile(
    r"^\s*$"
    r"|^This product includes software developed (at|by)"
    r"|^The Apache Software Foundation \(https?://www\.apache\.org/?\)\.?$"
    r"|^\(https?://www\.apache\.org/?\)\.?$"
    r"|Copyright [0-9 ,-]+(-present)? The Apache Software Foundation"
    r"|^Apache [A-Za-z0-9 ()]+$",
    re.IGNORECASE)
# group:artifact:version anywhere in a line. The group must contain a dot and the version must
# start with a digit, which keeps URLs and host:port pairs out.
ENTRY = re.compile(r"(?<![\w/.:-])([A-Za-z][\w-]*(?:\.[\w-]+)+):([\w.-]+):(\d[\w.-]*)")
LICENSE_REF = re.compile(r"(?<![/\w])licenses/([A-Za-z0-9._-]+)")

failures = []
leads = []


BUILD = ["mvn", "clean", "package", "-DskipTests", "-pl", "gremlin-driver,gremlin-console,gremlin-server", "-am"]
BUILD_LOG = "target/license-inventory-build.log"

# key -> (path template with {v} for the version, LICENSE/NOTICE source directory)
ARTIFACTS = {
    "shaded": ("gremlin-shaded/target/gremlin-shaded-{v}.jar", "gremlin-shaded/src/main/static"),
    "driver": ("gremlin-driver/target/gremlin-driver-{v}-shaded.jar", "gremlin-driver/src/main/static"),
    "console-uber": ("gremlin-console/target/gremlin-console-{v}-uber.jar", "gremlin-console/src/main/static"),
    "server-zip": ("gremlin-server/target/apache-tinkerpop-gremlin-server-{v}-distribution.zip",
                   "gremlin-server/src/main/static"),
    "console-zip": ("gremlin-console/target/apache-tinkerpop-gremlin-console-{v}-distribution.zip",
                    "gremlin-console/src/main/static"),
}
# Language-variant packages: package directory, and how LICENSE and NOTICE get into what is
# published. "npm" asks npm for the packed file list; a (file, pattern) pair is a packaging file
# that must match the pattern for each name; None means the whole directory is published, as a Go
# module is.
GLVS = {
    "gremlin-python": ("gremlin-python/src/main/python",
                       ("gremlin-python/src/main/python/MANIFEST.in", r"(?m)^include {name}\s*$")),
    "gremlin-javascript": ("gremlin-javascript/src/main/javascript/gremlin-javascript", "npm"),
    "gremlin-dotnet": ("gremlin-dotnet",
                       ("gremlin-dotnet/src/Gremlin.Net/Gremlin.Net.csproj", r'Include="[^"]*/{name}"\s+Pack="true"')),
    "gremlin-go": ("gremlin-go", None),
    "gremlint": ("gremlint", "npm"),
}
APACHE_END = "limitations under the License."
KEYS = list(ARTIFACTS) + ["source", "glv"]


def pom_version():
    """The project version declared in the root pom.xml, exactly as written."""
    root = ElementTree.parse("pom.xml").getroot()
    version = next((c.text.strip() for c in root if _local(c.tag) == "version" and c.text), None)
    if not version or "$" in version:
        sys.exit("Cannot read a literal <version> from the root pom.xml (found %r)." % version)
    return version


def build():
    """Run the fresh build. Exits with the tail of the log if it fails. Returns its start time."""
    print("Building: %s" % " ".join(BUILD))
    print("  log: %s" % BUILD_LOG)
    started = time.time()
    # mvn clean deletes the root target/ during the build, so log to a temporary file and copy it
    # into place afterwards.
    with tempfile.NamedTemporaryFile("w+", encoding="utf-8", errors="replace", suffix=".log") as log:
        code = subprocess.run(BUILD, stdout=log, stderr=subprocess.STDOUT).returncode
        log.flush()
        os.makedirs(os.path.dirname(BUILD_LOG), exist_ok=True)
        shutil.copyfile(log.name, BUILD_LOG)
    print("  finished in %d s with exit code %d" % (time.time() - started, code))
    if code != 0:
        with open(BUILD_LOG, encoding="utf-8", errors="replace") as log:
            print("".join(log.readlines()[-30:]))
        sys.exit("The build failed, so nothing was checked. See %s." % BUILD_LOG)
    return started


def entry_prefix(z, path):
    """Where LICENSE, NOTICE and licenses/ sit: META-INF/ in a jar, the top directory in a zip."""
    if path.endswith(".jar"):
        return "META-INF/"
    tops = {n.split("/", 1)[0] for n in z.namelist() if "/" in n}
    return (tops.pop() + "/") if len(tops) == 1 else ""


def fail(msg):
    failures.append(msg)
    print("  FAIL " + msg)


def read(path):
    with open(path, encoding="utf-8") as f:
        return f.read()


def pom_info(z, name):
    """(group, artifact, version, [license names]) for each embedded Maven pom in zip z."""
    found = []
    for n in z.namelist():
        if not (n.startswith("META-INF/maven/") and n.endswith("/pom.properties")):
            continue
        props = dict(line.split("=", 1) for line in z.read(n).decode("utf-8", "replace").splitlines()
                     if "=" in line and not line.startswith("#"))
        licenses = []
        pom = n[:-len("pom.properties")] + "pom.xml"
        if pom in z.namelist():
            licenses = pom_licenses(z.read(pom))
        found.append((props.get("groupId", "?"), props.get("artifactId", "?"), props.get("version", "?"), licenses))
    return found


M2 = os.path.expanduser("~/.m2/repository")


def _local(tag):
    return tag.rsplit("}", 1)[-1]


def pom_licenses(data, depth=0):
    """License names declared by a pom, following <parent> poms in the local Maven repository."""
    try:
        root = ElementTree.fromstring(data)
    except ElementTree.ParseError:
        return []
    names = [n.text.strip() for lic in root.iter() if _local(lic.tag) == "license"
             for n in lic if _local(n.tag) == "name" and n.text]
    if names or depth > 5:
        return names
    parent = next((c for c in root if _local(c.tag) == "parent"), None)
    if parent is None:
        return []
    gav = {_local(c.tag): (c.text or "").strip() for c in parent}
    path = os.path.join(M2, *gav.get("groupId", "").split("."), gav.get("artifactId", ""), gav.get("version", ""),
                        "%s-%s.pom" % (gav.get("artifactId", ""), gav.get("version", "")))
    if not os.path.isfile(path):
        return []
    with open(path, "rb") as f:
        return pom_licenses(f.read(), depth + 1)


_m2_index = None


def m2_lookup(jar_name):
    """(group, artifact, version, pom licenses) for a jar with no embedded pom, found by file name
    in the local Maven repository. The index is built once, on first use."""
    global _m2_index
    if _m2_index is None:
        _m2_index = {}
        for root, _, files in os.walk(M2):
            for f in files:
                if f.endswith(".jar"):
                    _m2_index.setdefault(f, root)
    root = _m2_index.get(jar_name)
    if not root:
        return None
    rel = os.path.relpath(root, M2).split(os.sep)
    if len(rel) < 3:
        return None
    group, artifact, version = ".".join(rel[:-2]), rel[-2], rel[-1]
    pom = os.path.join(root, "%s-%s.pom" % (artifact, version))
    licenses = []
    if os.path.isfile(pom):
        with open(pom, "rb") as f:
            licenses = pom_licenses(f.read())
    return group, artifact, version, licenses


def sniffed_license(z):
    """A license name from the jar's manifest or LICENSE text, for jars whose pom says nothing."""
    if "META-INF/MANIFEST.MF" in z.namelist():
        m = re.search(r"^Bundle-License:\s*(.+)$", z.read("META-INF/MANIFEST.MF").decode("utf-8", "replace"),
                      re.MULTILINE)
        if m:
            return [m.group(1).strip()]
    for n in z.namelist():
        if re.fullmatch(r"META-INF/LICENSE(\.txt|\.md)?", n, re.IGNORECASE):
            head = z.read(n)[:2000].decode("utf-8", "replace")
            for name in ("Apache License", "MIT License", "BSD", "Eclipse Public License"):
                if name.lower() in head.lower():
                    return [name + " (from META-INF/LICENSE)"]
    return []


def notice_extra(z):
    for n in z.namelist():
        if re.fullmatch(r"META-INF/NOTICE(\.txt|\.md)?", n, re.IGNORECASE):
            lines = z.read(n).decode("utf-8", "replace").replace("\r", "").splitlines()
            return [l for l in lines if not BOILERPLATE.search(l)]
    return None


def _normalized(data):
    return re.sub(r"\s+", " ", data.decode("utf-8", "replace")).strip()


def nested_license_files(z):
    return sorted(n for n in z.namelist()
                  if re.search(r"(^|/)[^/]*(LICENSE|NOTICE|COPYING)[^/]*$", n, re.IGNORECASE)
                  and not n.endswith(".class")
                  or re.match(r"META-INF/licenses?/[^/]+$", n, re.IGNORECASE))


def relocated_roots(z):
    roots = {}
    for n in z.namelist():
        if not n.endswith(".class") or n.startswith("META-INF/"):
            continue
        parts = n.split("/")[:-1]
        at = [i for i, part in enumerate(parts[:-1]) if part == "shaded" or "jarjar" in part]
        for i in at:
            # The outer relocation names the dependency. A relocation nested inside it names a
            # sub-component, so keep enough of its package to show what it is (e.g. objectweb/asm).
            key = "/".join(parts[:i + (2 if i == at[0] else 4)])
            roots[key] = roots.get(key, 0) + 1
    return roots


def class_roots(z):
    roots = {}
    for n in z.namelist():
        if n.endswith(".class") and not n.startswith("META-INF/") and not n.startswith("org/apache/tinkerpop/gremlin/"):
            parts = n.split("/")
            depth = 5 if n.startswith("org/apache/tinkerpop/shaded/") else 3
            key = "/".join(parts[:min(depth, len(parts) - 1)])
            roots[key] = roots.get(key, 0) + 1
    return roots


def check_packed(z, prefix, static):
    for f in ("LICENSE", "NOTICE"):
        name = prefix + f
        if name not in z.namelist():
            fail("%s not packed" % name)
        elif z.read(name).decode("utf-8") != read(os.path.join(static, f)):
            fail("packed %s differs from %s/%s" % (name, static, f))
        else:
            print("  ok   packed %s matches %s/%s" % (f, static, f))
    refs = set(LICENSE_REF.findall(read(os.path.join(static, "LICENSE"))))
    packed = {n[len(prefix + "licenses/"):] for n in z.namelist()
              if n.startswith(prefix + "licenses/") and n != prefix + "licenses/" and "/" not in n[len(prefix + "licenses/"):]}
    for r in sorted(refs - packed):
        fail("LICENSE refers to licenses/%s, which is not packed" % r)
    for p in sorted(packed - refs):
        fail("licenses/%s is packed but LICENSE never refers to it" % p)
    if refs == packed:
        print("  ok   %d licenses/ files, all referenced" % len(packed))


def merged_into(z, g, a, ver):
    """True when z holds the classes of g:a:ver merged in without its pom metadata, as an uber jar
    does for some dependencies. Compared against the original jar in the local Maven repository."""
    path = m2_jar(g, a, ver)
    if not path:
        return False
    with zipfile.ZipFile(path) as dep:
        classes = [n for n in dep.namelist() if n.endswith(".class") and not n.startswith("META-INF/")]
    if not classes:
        return False
    names = set(z.namelist())
    return sum(1 for c in classes if c in names) >= 0.9 * len(classes)


def check_entries(static, bundled, z=None):
    """Every (group:artifact:version) LICENSE entry must name something bundled, at that version."""
    by_artifact = {}
    for g, a, ver in bundled:
        by_artifact.setdefault((g, a), set()).add(ver)
    for g, a, ver in ENTRY.findall(read(os.path.join(static, "LICENSE"))):
        have = by_artifact.get((g, a))
        if not have and z is not None and merged_into(z, g, a, ver):
            print("  ok   LICENSE entry %s:%s:%s is bundled (classes merged in without its pom)" % (g, a, ver))
            continue
        if not have:
            fail("LICENSE lists %s:%s:%s, which is not bundled" % (g, a, ver))
        elif ver not in have:
            fail("LICENSE lists %s:%s:%s, but %s is bundled" % (g, a, ver, ", ".join(sorted(have))))
        else:
            print("  ok   LICENSE entry %s:%s:%s is bundled" % (g, a, ver))


def mentioned(static, group, artifact):
    """True when LICENSE names the artifact by its full artifactId, as a whole word. A mention in
    NOTICE alone does not count: a non-Apache component needs its LICENSE entry."""
    text = read(os.path.join(static, "LICENSE")).lower()
    return re.search(r"(?<![\w.-])%s(?![\w-])" % re.escape(artifact.lower()), text) is not None


def flag(static, group, artifact, licenses):
    """Flag only what may need a LICENSE entry: a non-Apache or unknown license that LICENSE never names."""
    if mentioned(static, group, artifact):
        return ""
    joined = " ".join(licenses).lower()
    if "apache" in joined:
        return ""
    leads.append("%s:%s" % (group, artifact))
    if group.startswith(ASF_GROUP_PREFIXES):
        return "  [license not found in pom: confirm]"
    return "  [NOT IN LICENSE: %s]" % ("license unknown, check the jar" if not licenses else "non-Apache")


def m2_jar(g, a, ver):
    path = os.path.join(M2, *g.split("."), a, ver, "%s-%s.jar" % (a, ver))
    return path if os.path.isfile(path) else None


def dropped_license_files(g, a, ver, z):
    """License files inside the original dependency jar whose text the shaded jar does not carry,
    under any name. A text TinkerPop packs under its own name in licenses/ counts as carried."""
    path = m2_jar(g, a, ver)
    if not path:
        return None
    carried = {_normalized(z.read(n)) for n in nested_license_files(z)}
    with zipfile.ZipFile(path) as dep:
        return [f for f in nested_license_files(dep)
                if not re.fullmatch(r"META-INF/(LICENSE|NOTICE)(\.txt|\.md)?", f, re.IGNORECASE)
                and _normalized(dep.read(f)) not in carried]


def coordinate_free_entries(static):
    """LICENSE entries with no group:artifact:version, which no jar check can confirm."""
    out = []
    for line in read(os.path.join(static, "LICENSE")).splitlines():
        if re.match(r"^\s{5}\S", line) and not ENTRY.search(line):
            out.append(line.strip())
    return out


def report_jar(label, z, static, indent="  "):
    poms = pom_info(z, label)
    print("%sbundled Maven artifacts:" % indent)
    for g, a, ver, lic in sorted(poms):
        if g == TP_GROUP:
            continue
        asf = g.startswith(ASF_GROUP_PREFIXES)
        if not lic and m2_jar(g, a, ver):
            with zipfile.ZipFile(m2_jar(g, a, ver)) as dep:
                lic = sniffed_license(dep)
        print("%s  %s:%s:%s  license=%s%s%s" % (indent, g, a, ver, "|".join(lic) or "?",
                                            "  [ASF]" if asf else "", flag(static, g, a, lic)))
    roots = class_roots(z)
    print("%sclass roots outside org/apache/tinkerpop/gremlin (classes):" % indent)
    for k in sorted(roots):
        print("%s  %-60s %d" % (indent, k, roots[k]))
    for g, a, ver, _ in sorted(poms):
        if g == TP_GROUP:
            continue
        dropped = dropped_license_files(g, a, ver, z)
        if dropped:
            leads.append("%s:%s sub-licenses" % (g, a))
            print("%s  sub-licenses in %s:%s:%s not carried into this jar: %s" % (indent, g, a, ver, ", ".join(dropped)))
    rel = relocated_roots(z)
    if rel:
        print("%srelocated sub-component packages (look for their licenses):" % indent)
        for k in sorted(rel):
            print("%s  %-60s %d" % (indent, k, rel[k]))
    return [(g, a, ver) for g, a, ver, _ in poms]


def report_zip(z, static):
    bundled, rows = [], []
    for n in sorted(z.namelist()):
        if not n.endswith(".jar"):
            continue
        inner = zipfile.ZipFile(io.BytesIO(z.read(n)))
        name = n.rsplit("/", 1)[-1]
        poms = pom_info(inner, name)
        if not poms:
            found = m2_lookup(name)
            if found:
                poms = [found]
        if not any(ls for _, _, _, ls in poms):
            sniffed = sniffed_license(inner)
            poms = [(g, a, v, sniffed) for g, a, v, _ in poms] or [("?", name.rsplit("-", 1)[0], "?", sniffed)]
        bundled += [(g, a, ver) for g, a, ver, _ in poms]
        if name.startswith(("gremlin-", "tinkergraph-", "hadoop-gremlin", "spark-gremlin", "neo4j-gremlin")):
            continue
        lic = "|".join(sorted({l for _, _, _, ls in poms for l in ls})) or "?"
        extra = notice_extra(inner)
        files = [f for f in nested_license_files(inner) if not re.fullmatch(r"META-INF/(LICENSE|NOTICE)(\.txt)?", f)]
        # A jar can embed other artifacts' poms (jansi embeds hawtjni-runtime). Judge each one on
        # its own declared license, so one Apache pom does not cover the rest.
        flags = [(a, flag(static, g, a, ls)) for g, a, _, ls in poms]
        men = "".join(f if len(poms) == 1 else f.replace("[", "[%s: " % a, 1) for a, f in flags if f)
        rows.append((name, lic, extra, files, men, relocated_roots(inner)))
    print("  bundled jars (excluding TinkerPop's own):")
    for name, lic, extra, files, men, rel in rows:
        print("    %s  license=%s%s" % (name, lic, men))
        if extra is None:
            print("      NOTICE: none")
        elif extra:
            print("      NOTICE: %d non-boilerplate line(s): %s" % (len(extra), " / ".join(l.strip() for l in extra[:3])[:160]))
        else:
            print("      NOTICE: boilerplate only")
        if files:
            print("      extra license files: %s" % ", ".join(files))
        for k in sorted(rel):
            print("      relocated: %s (%d classes)" % (k, rel[k]))
    web = [n for n in z.namelist() if re.search(r"\.(css|js|html?)$", n, re.IGNORECASE)]
    print("  loose web files: %s" % (", ".join(web) if web else "none"))
    return bundled


def report_source():
    print("\n== source distribution (root LICENSE, licenses/)")
    text = read("LICENSE")
    refs = set(LICENSE_REF.findall(text))
    have = set(os.listdir("licenses")) if os.path.isdir("licenses") else set()
    for r in sorted(refs - have):
        fail("root LICENSE refers to licenses/%s, which does not exist" % r)
    for h in sorted(have - refs):
        fail("licenses/%s exists but the root LICENSE never refers to it" % h)
    tracked = subprocess.run(["git", "ls-files"], capture_output=True, text=True).stdout.splitlines()
    web = [t for t in tracked if re.search(r"\.(css|js|html?|scss)$", t, re.IGNORECASE)
           and not t.startswith(("licenses/", ".beads/", ".skills/"))]
    manifests = {t: read(t).lower() for t in tracked if t.endswith("package.json") and "node_modules" not in t}
    print("  root LICENSE entries and the tracked files that look like them (verify each is really vendored):")
    for line in text.splitlines():
        m = re.match(r"^\s+(\S+)", line)
        if not m or not LICENSE_REF.search(line):
            continue
        token = re.sub(r"[^a-z0-9]", "", m.group(1).lower().split(".")[0])
        hits = [t for t in web if token and token in re.sub(r"[^a-z0-9]", "", t.lower().rsplit("/", 1)[-1])]
        if not hits and token and web:
            grep = subprocess.run(["git", "grep", "-l", "-i", "-w", "-F", token, "--"] + web,
                                  capture_output=True, text=True).stdout.split()
            hits = ["%s (content)" % h for h in grep]
        npm = [t for t, body in manifests.items()
               if re.search(r'"%s"\s*:' % re.escape(m.group(1).lower().split(".")[0]), body)]
        print("    %-40s -> %s" % (m.group(1), ", ".join(hits[:4]) if hits else "NO MATCHING FILE"))
        if npm:
            print("    %-40s    declared as an npm dependency in %s, so a hit may only import it" % ("", ", ".join(npm)))


def report_glv():
    """Each language-variant package publishes its own LICENSE and NOTICE. It declares its
    dependencies rather than bundling them, so anything beyond the Apache license is a lead."""
    for name, (d, packing) in GLVS.items():
        print("\n== glv %s: %s" % (name, d))
        files = {f: os.path.join(d, f) for f in ("LICENSE", "NOTICE")}
        missing = [f for f, path in files.items() if not os.path.isfile(path)]
        for f in missing:
            fail("%s/%s does not exist" % (d, f))
        if missing:
            continue
        # Does the published package carry both files?
        if packing == "npm":
            out = subprocess.run(["npm", "pack", "--dry-run", "--json", "--ignore-scripts"], cwd=d,
                                 capture_output=True, text=True) if shutil.which("npm") else None
            if out is None or out.returncode != 0:
                print("  not checked: npm pack --dry-run did not run")
            else:
                packed = {f["path"] for f in json.loads(out.stdout)[0]["files"]}
                for f in files:
                    if f in packed:
                        print("  ok   npm package includes %s" % f)
                    else:
                        fail("npm package for %s does not include %s" % (d, f))
        elif packing:
            path, pattern = packing
            text = read(path)
            for f in files:
                if re.search(pattern.format(name=f), text):
                    print("  ok   %s packs %s" % (path, f))
                else:
                    fail("%s does not pack %s" % (path, f))
        else:
            print("  ok   the whole directory is published, LICENSE and NOTICE included")
        license_text = read(files["LICENSE"])
        refs = set(LICENSE_REF.findall(license_text))
        for r in sorted(refs):
            if not os.path.isfile(os.path.join(d, "licenses", r)):
                fail("%s/LICENSE refers to licenses/%s, which the package does not have" % (d, r))
        extra = [l.strip() for l in license_text.split(APACHE_END, 1)[-1].splitlines() if l.strip()]
        if extra:
            leads.append(name + " LICENSE")
            print("  LICENSE has entries beyond the Apache license (confirm each is shipped):")
            for l in extra:
                print("    " + l[:150])
        notice = [l.strip() for l in read(files["NOTICE"]).splitlines()
                  if not BOILERPLATE.search(l) and l.strip() != "Apache TinkerPop"]
        if notice:
            leads.append(name + " NOTICE")
            print("  NOTICE has non-boilerplate lines: %s" % " / ".join(notice)[:160])
        # Third-party copyright lines in what the package tracks are vendored code.
        grep = subprocess.run(["git", "grep", "-I", "-i", "-n", "copyright", "--", d,
                               ":!%s/LICENSE" % d, ":!%s/NOTICE" % d],
                              capture_output=True, text=True).stdout.splitlines()
        foreign = [g for g in grep if not re.search(
            r"apache software foundation|regarding copyright ownership|/(tests?|Test[^/]*)/", g, re.IGNORECASE)]
        if foreign:
            leads.append(name + " copyright")
            print("  non-ASF copyright lines in tracked files (vendored code?):")
            for g in foreign[:10]:
                print("    " + g[:150])


def keys(value):
    chosen = [k for k in value.split(",") if k]
    unknown = [k for k in chosen if k not in KEYS]
    if not chosen or unknown:
        raise argparse.ArgumentTypeError("unknown artifact key(s) %s; choose from %s"
                                         % (", ".join(unknown) or "(none)", ", ".join(KEYS)))
    return set(chosen)


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--strict", action="store_true", help="exit 1 on any mechanical failure")
    parser.add_argument("--skip-build", action="store_true",
                        help="check the artifacts already in target/ instead of building fresh ones")
    parser.add_argument("--only", type=keys, metavar="KEYS", default=set(KEYS),
                        help="comma-separated subset of: " + ", ".join(KEYS))
    args = parser.parse_args()
    if not os.path.isfile("pom.xml") or not os.path.isdir(".skills"):
        sys.exit("Run from the TinkerPop repository root.")
    version = pom_version()
    print("TinkerPop %s (from pom.xml)" % version)
    binaries = [k for k in ARTIFACTS if k in args.only]
    started = None
    if binaries and args.skip_build:
        missing = [ARTIFACTS[k][0].format(v=version) for k in binaries
                   if not os.path.isfile(ARTIFACTS[k][0].format(v=version))]
        if missing:
            print("--skip-build, but these %s artifacts are not in target/:" % version)
            for m in missing:
                print("  " + m)
            print("Nothing was checked. Run without --skip-build to build them.")
            sys.exit(2)
        print("Skipping the build: checking the artifacts already in target/, which may be stale.")
    elif binaries:
        started = build()
    for key in binaries:
        template, static = ARTIFACTS[key]
        path = template.format(v=version)
        print("\n== %s: %s" % (key, path))
        if not os.path.isfile(path):
            fail("%s was not produced by the build" % path)
            continue
        if started is None:
            print("  built %s (not rebuilt by this run)"
                  % time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(os.path.getmtime(path))))
        elif os.path.getmtime(path) < started:
            fail("%s predates this run's build, so it is stale" % path)
            continue
        with zipfile.ZipFile(path) as z:
            prefix = entry_prefix(z, path)
            check_packed(z, prefix, static)
            bundled = report_zip(z, static) if path.endswith(".zip") else report_jar(key, z, static)
            check_entries(static, bundled, None if path.endswith(".zip") else z)
            free = coordinate_free_entries(static)
            if free:
                print("  LICENSE entries without Maven coordinates (confirm each is in this artifact):")
                for e in free:
                    print("    " + e[:150])
    if "source" in args.only:
        report_source()
    if "glv" in args.only:
        report_glv()
    print("\n%d mechanical failure(s), %d lead(s) to review" % (len(failures), len(leads)))
    if args.strict and failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
