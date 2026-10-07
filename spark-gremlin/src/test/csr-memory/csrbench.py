#!/usr/bin/env python3
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
"""
Driver for the CSR memory benchmark (WP32 of the CSR spike).

Launches CsrMemoryRun, one JVM per measurement, from a matrix file, samples
process memory from outside, parses GC logs and NMT output, and writes
result.json per run plus runs.csv and summary.csv.  Python 3.8+, standard
library only.  See README.md in this directory.
"""

import argparse
import csv
import filecmp
import datetime
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time

MAIN_CLASS = "org.apache.tinkerpop.gremlin.spark.csr.CsrMemoryRun"
IS_LINUX = sys.platform.startswith("linux")
HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(HERE, "..", "..", "..", ".."))
DEFAULT_QUERY_FILE = os.path.join(
    REPO_ROOT, "spark-gremlin", "src", "test", "resources", "org", "apache", "tinkerpop",
    "gremlin", "spark", "csr", "csr-queries.txt")
DEFAULT_CLASSPATH_FILE = os.path.join(REPO_ROOT, "spark-gremlin", "target", "csrbench-classpath.txt")
MB = 1 << 20
GB = 1 << 30
CSR_SYSTEMS = ("csr", "csr-native", "csr-facade")

ADD_OPENS = [
    "java.base/java.util.concurrent.atomic", "java.base/java.util", "java.base/java.lang",
    "java.base/java.nio", "java.base/sun.nio.ch", "java.base/java.lang.invoke",
]

OS_COLUMNS = ["epochMillis", "phase", "vmRSS", "rssAnon", "rssFile", "rssShmem", "vmSize", "vmSwap",
              "vmHWM", "cgroupCurrent", "cgroupAnon", "cgroupFile",
              "smapsRss", "smapsPss", "smapsPssAnon", "smapsPssFile"]


def log(msg):
    print(msg, flush=True)


def warn(msg):
    print("WARNING: " + msg, file=sys.stderr, flush=True)


def safe(s):
    return re.sub(r"[^A-Za-z0-9._-]+", "_", str(s)).strip("_")


def human_bytes(n):
    n = int(n)
    if n % GB == 0:
        return "%dGB" % (n // GB)
    if n % MB == 0:
        return "%dMB" % (n // MB)
    return "%dB" % n


def read_text(path):
    with open(path, "r") as f:
        return f.read()


def read_int(path):
    try:
        return int(read_text(path).strip())
    except (OSError, ValueError):
        return None


def kv_file(path):
    out = {}
    try:
        for line in read_text(path).splitlines():
            parts = line.split()
            if len(parts) >= 2:
                try:
                    out[parts[0]] = int(parts[1])
                except ValueError:
                    pass
    except OSError:
        pass
    return out


def mean(xs):
    return sum(xs) / float(len(xs)) if xs else None


# ---------------------------------------------------------------------------
# matrix expansion
# ---------------------------------------------------------------------------

def parse_query_file(path):
    """Returns list of (id, size, systems) where systems is None for '*'."""
    out = []
    with open(path, "r") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            parts = [p.strip() for p in s.split("|", 3)]
            if len(parts) < 4:
                continue
            systems = None if parts[2] == "*" else [x.strip() for x in parts[2].split(",") if x.strip()]
            out.append((parts[0], parts[1], systems))
    return out


def dataset_name(dataset, snapshot):
    p = dataset or snapshot or "none"
    b = os.path.basename(os.path.normpath(p))
    return re.sub(r"\.kryo$", "", b)


def expand_matrix(matrix, only=None):
    """Expands the matrix into a list of run descriptors (dicts)."""
    def opt(entry, key, default=None):
        return entry.get(key, matrix.get(key, default))

    query_cache = {}
    runs = []
    for idx, e in enumerate(matrix.get("runs", [])):
        mode = e["mode"]
        if mode not in ("build", "open", "query"):
            raise ValueError("runs[%d]: unknown mode %r" % (idx, mode))
        dataset = e.get("dataset")
        snapshot = e.get("snapshot")
        base = {
            "entryIndex": idx, "mode": mode, "dataset": dataset, "snapshot": snapshot,
            "datasetName": dataset_name(dataset, snapshot),
            "cold": bool(e.get("cold", False)),
            "timeoutSeconds": opt(e, "timeoutSeconds", 7200),
            "extraJvmArgs": list(e.get("jvmArgs", [])),
            "entry": e,
        }
        if mode == "build":
            builder = e.get("builder", "streaming")
            source = e.get("source", "gryo")
            # budgetOfPeak: a fraction of the peak budget use of the latest unconstrained hybrid build of the same
            # dataset, resolved when the run starts (see resolve_relative_budget)
            of_peak = e.get("budgetOfPeak")
            budget = None if of_peak is not None else int(e.get("builderBudget", 256 * MB))
            label = "%dpct" % round(float(of_peak) * 100) if of_peak is not None else human_bytes(budget)
            query = "b%s-%s" % (label, source) + ("-" + e["tag"] if e.get("tag") else "")
            r = dict(base, system=builder, query=query,
                     params={"builder": builder, "source": source, "builderBudget": budget,
                             "budgetOfPeak": of_peak, "tag": e.get("tag")})
            runs.append(r)
        elif mode == "open":
            systems = e.get("systems") or [e["system"]]
            for s in systems:
                r = dict(base, system=s, query="cold" if base["cold"] else "warm",
                         params={"verifyChecksums": bool(e.get("verifyChecksums", False))})
                runs.append(r)
        else:
            systems = e.get("systems") or [e["system"]]
            qfile = opt(e, "queryFile", DEFAULT_QUERY_FILE)
            qids = list(e.get("queries", ["*"]))
            if "*" in qids:
                if qfile not in query_cache:
                    if not os.path.exists(qfile):
                        raise ValueError("query file %s not found; needed to expand '*' (runs[%d])" % (qfile, idx))
                    query_cache[qfile] = parse_query_file(qfile)
                catalog = query_cache[qfile]
            else:
                catalog = None
            for s in systems:
                ids = []
                if catalog is not None:
                    for qid, _size, qsys in catalog:
                        if qsys is not None and s not in qsys and opt(e, "skipUnsupported", False):
                            continue
                        ids.append(qid)
                    ids += [q for q in qids if q != "*" and q not in ids]
                else:
                    ids = qids
                for qid in ids:
                    r = dict(base, system=s, query=qid, queryFile=qfile,
                             params={"csrBudget": int(opt(e, "csrBudget", GB)), "warm": int(opt(e, "warm", 3))})
                    runs.append(r)
    for r in runs:
        r["baseId"] = safe("-".join([r["mode"], r["system"], r["datasetName"], r["query"]]))
    if only:
        rx = re.compile(only)
        runs = [r for r in runs if rx.search(r["baseId"])]
    return runs


def build_command(run, matrix, run_dir, work_dir, classpath, cgroup_dir):
    e = run["entry"]

    def opt(key, default=None):
        return e.get(key, matrix.get(key, default))

    jvm = [opt("java", "java"), "-XX:+UnlockDiagnosticVMOptions",
           "-Xlog:gc*:file=%s:time,uptime,level,tags" % os.path.join(run_dir, "gc.log"),
           "-XX:NativeMemoryTracking=summary", "-XX:+PrintNMTStatistics"]
    if matrix.get("heapDumpOnOom", False):
        jvm += ["-XX:+HeapDumpOnOutOfMemoryError", "-XX:HeapDumpPath=" + run_dir]
    jvm += ["--add-opens=%s=ALL-UNNAMED" % m for m in ADD_OPENS]
    jvm += list(matrix.get("jvmArgs", [])) + run["extraJvmArgs"]
    jvm += ["-cp", classpath, MAIN_CLASS]

    mode, system = run["mode"], run["system"]
    a = ["--mode", mode]
    if mode == "build":
        a += ["--dataset", run["dataset"], "--snapshot", run["snapshot"],
              "--builder", run["params"]["builder"], "--source", run["params"]["source"],
              "--builder-budget", str(run["params"]["builderBudget"])
              if run["params"]["builderBudget"] is not None else "<budgetOfPeak>"]
    else:
        a += ["--system", system]
        if system in CSR_SYSTEMS:
            if run["snapshot"]:
                a += ["--snapshot", run["snapshot"]]
        elif run["dataset"]:
            a += ["--dataset", run["dataset"]]
        if mode == "open":
            a += ["--verify-checksums", "true" if run["params"]["verifyChecksums"] else "false"]
        else:
            a += ["--query-file", run["queryFile"], "--query", run["query"],
                  "--warm", str(run["params"]["warm"]), "--csr-budget", str(run["params"]["csrBudget"])]
    timeout = run["timeoutSeconds"]
    soft = opt("softTimeoutSeconds", timeout - 60 if timeout and timeout > 180 else None)
    if soft:
        a += ["--timeout-seconds", str(int(soft))]
    if opt("sparkMaster"):
        a += ["--spark-master", opt("sparkMaster")]
    for k, v in (opt("sparkConf", {}) or {}).items():
        a += ["--spark-conf", "%s=%s" % (k, v)]
    a += ["--work-dir", work_dir,
          "--sample-interval-ms", str(int(float(opt("sampleIntervalSeconds", 1)) * 1000)),
          "--series-out", os.path.join(run_dir, "heap-series.csv"),
          "--out", os.path.join(run_dir, "memoryrun.json")]
    cmd = jvm + a
    if cgroup_dir:
        cmd = ["sh", "-c",
               'echo $$ > "$0/cgroup.procs" || echo CSRBENCH-CGROUP-JOIN-FAILED >&2; exec "$@"',
               cgroup_dir] + cmd
    return cmd


# ---------------------------------------------------------------------------
# Linux helpers: cache drop, cgroups
# ---------------------------------------------------------------------------

_warned = set()


def warn_once(key, msg):
    if key not in _warned:
        _warned.add(key)
        warn(msg)


def drop_caches():
    """Returns (done, note)."""
    if not IS_LINUX:
        warn_once("dc-os", "page cache drop is only supported on Linux; cold runs are not cold")
        return False, "not linux"
    if os.geteuid() != 0:
        warn_once("dc-root", "not root: cannot drop page cache; cold runs are not cold (run with sudo)")
        return False, "not root"
    try:
        os.sync()
        with open("/proc/sys/vm/drop_caches", "w") as f:
            f.write("3\n")
        return True, "ok"
    except OSError as ex:
        warn_once("dc-fail", "drop_caches failed: %s" % ex)
        return False, str(ex)


def prewarm(path):
    """Reads all files below path so a warm open starts with a warm page cache."""
    if not path or not os.path.exists(path):
        return
    paths = [path] if os.path.isfile(path) else [os.path.join(d, f) for d, _s, fs in os.walk(path) for f in fs]
    for p in paths:
        try:
            with open(p, "rb") as f:
                while f.read(8 * MB):
                    pass
        except OSError:
            pass


class CgroupManager(object):
    def __init__(self, enabled, root="/sys/fs/cgroup", parent="csrbench"):
        self.root = root
        self.parent_rel = parent
        self.parent = os.path.join(root, parent)
        self.ok = False
        self.note = "disabled"
        if not enabled:
            return
        if not IS_LINUX:
            self.note = "not linux"
            warn("cgroup requested but not on Linux; continuing without")
            return
        if not os.path.exists(os.path.join(root, "cgroup.controllers")):
            self.note = "cgroup v2 not mounted at " + root
            warn(self.note + "; continuing without cgroups")
            return
        try:
            path = root
            self._enable(path)
            for comp in [c for c in parent.split("/") if c]:
                path = os.path.join(path, comp)
                os.makedirs(path, exist_ok=True)
                self._enable(path)
            if "memory" not in read_text(os.path.join(self.parent, "cgroup.controllers")).split():
                raise OSError("memory controller not available in " + self.parent)
            self.ok = True
            self.note = "ok"
        except OSError as ex:
            self.note = "cannot set up cgroup parent %s: %s" % (self.parent, ex)
            warn(self.note + " (needs root or a delegated cgroup); continuing without cgroups")

    @staticmethod
    def _enable(path):
        ctrl = read_text(os.path.join(path, "cgroup.controllers")).split()
        sub = [s.lstrip("+") for s in read_text(os.path.join(path, "cgroup.subtree_control")).split()]
        if "memory" in ctrl and "memory" not in sub:
            with open(os.path.join(path, "cgroup.subtree_control"), "w") as f:
                f.write("+memory")

    def create(self, name):
        if not self.ok:
            return None
        path = os.path.join(self.parent, name)
        try:
            os.mkdir(path)
            if not os.path.exists(os.path.join(path, "memory.current")):
                raise OSError("no memory.current in new cgroup")
            return path
        except OSError as ex:
            warn("cannot create cgroup %s: %s; continuing without" % (path, ex))
            try:
                os.rmdir(path)
            except OSError:
                pass
            return None

    @staticmethod
    def kill(path):
        try:
            with open(os.path.join(path, "cgroup.kill"), "w") as f:
                f.write("1")
        except OSError:
            pass

    @staticmethod
    def remove(path):
        for _ in range(25):
            try:
                os.rmdir(path)
                return True
            except OSError:
                time.sleep(0.2)
        warn("could not remove cgroup " + path)
        return False


# ---------------------------------------------------------------------------
# sampler
# ---------------------------------------------------------------------------

class PhaseState(object):
    def __init__(self):
        self.lock = threading.Lock()
        self.current = ""
        self.events = []  # {phase, beginMillis, endMillis}

    def marker(self, millis, phase, kind):
        with self.lock:
            if kind == "begin":
                self.events.append({"phase": phase, "beginMillis": millis, "endMillis": None})
                self.current = phase
            else:
                for ev in reversed(self.events):
                    if ev["phase"] == phase and ev["endMillis"] is None:
                        ev["endMillis"] = millis
                        break
                self.current = ""

    def get(self):
        with self.lock:
            return self.current


class Sampler(threading.Thread):
    def __init__(self, pid, interval, smaps_interval, cgroup_dir, phases):
        threading.Thread.__init__(self)
        self.daemon = True
        self.pid = pid
        self.interval = max(0.05, float(interval))
        self.smaps_interval = float(smaps_interval) if smaps_interval else 0
        self.cg = cgroup_dir
        self.phases = phases
        self.rows = []
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        t0 = time.time()
        k = 0
        last_smaps = None
        while not self.stop_event.is_set():
            now = time.time()
            do_smaps = IS_LINUX and self.smaps_interval and (last_smaps is None or now - last_smaps >= self.smaps_interval - 1e-6)
            row = self.sample(do_smaps)
            if do_smaps:
                last_smaps = now
            if row is not None:
                self.rows.append(row)
            k += 1
            delay = t0 + k * self.interval - time.time()
            if delay < 0:
                k = int((time.time() - t0) / self.interval) + 1
                delay = 0
            self.stop_event.wait(delay)

    def sample(self, do_smaps):
        row = dict((c, None) for c in OS_COLUMNS)
        row["epochMillis"] = int(time.time() * 1000)
        row["phase"] = self.phases.get()
        if IS_LINUX:
            try:
                st = {}
                for line in read_text("/proc/%d/status" % self.pid).splitlines():
                    m = re.match(r"(VmRSS|RssAnon|RssFile|RssShmem|VmSize|VmSwap|VmHWM):\s+(\d+)\s*kB", line)
                    if m:
                        st[m.group(1)] = int(m.group(2)) * 1024
                row.update({"vmRSS": st.get("VmRSS"), "rssAnon": st.get("RssAnon"), "rssFile": st.get("RssFile"),
                            "rssShmem": st.get("RssShmem"), "vmSize": st.get("VmSize"),
                            "vmSwap": st.get("VmSwap"), "vmHWM": st.get("VmHWM")})
            except OSError:
                return None
            if self.cg:
                row["cgroupCurrent"] = read_int(os.path.join(self.cg, "memory.current"))
                stat = kv_file(os.path.join(self.cg, "memory.stat"))
                row["cgroupAnon"] = stat.get("anon")
                row["cgroupFile"] = stat.get("file")
            if do_smaps:
                try:
                    sm = {}
                    for line in read_text("/proc/%d/smaps_rollup" % self.pid).splitlines():
                        m = re.match(r"(Rss|Pss|Pss_Anon|Pss_File):\s+(\d+)\s*kB", line)
                        if m:
                            sm[m.group(1)] = int(m.group(2)) * 1024
                    row.update({"smapsRss": sm.get("Rss"), "smapsPss": sm.get("Pss"),
                                "smapsPssAnon": sm.get("Pss_Anon"), "smapsPssFile": sm.get("Pss_File")})
                except OSError:
                    pass
        else:
            try:
                out = subprocess.check_output(["ps", "-o", "rss=,vsz=", "-p", str(self.pid)],
                                              stderr=subprocess.DEVNULL).decode().split()
                if len(out) < 2:
                    return None
                row["vmRSS"] = int(out[0]) * 1024
                row["vmSize"] = int(out[1]) * 1024
            except (subprocess.CalledProcessError, OSError, ValueError):
                return None
        return row


def write_os_series(path, rows):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(OS_COLUMNS)
        for r in rows:
            w.writerow(["" if r.get(c) is None else r[c] for c in OS_COLUMNS])


def read_os_series(path):
    rows = []
    try:
        with open(path, "r", newline="") as f:
            for r in csv.DictReader(f):
                row = {}
                for c in OS_COLUMNS:
                    v = r.get(c)
                    if c == "phase":
                        row[c] = v or ""
                    else:
                        row[c] = int(v) if v not in (None, "") else None
                rows.append(row)
    except OSError:
        pass
    return rows


def os_stats(rows):
    def vals(k):
        return [r[k] for r in rows if r.get(k) is not None]

    out = {"samples": len(rows)}
    rss = [r for r in rows if r.get("vmRSS") is not None]
    if rss:
        peak = max(rss, key=lambda r: r["vmRSS"])
        out.update({"peakRss": peak["vmRSS"], "avgRss": mean(vals("vmRSS")),
                    "rssAnonAtPeak": peak.get("rssAnon"), "rssFileAtPeak": peak.get("rssFile"),
                    "rssShmemAtPeak": peak.get("rssShmem"),
                    "peakRssAnon": max(vals("rssAnon")) if vals("rssAnon") else None,
                    "peakRssFile": max(vals("rssFile")) if vals("rssFile") else None,
                    "avgRssAnon": mean(vals("rssAnon")), "avgRssFile": mean(vals("rssFile")),
                    "peakVmSize": max(vals("vmSize")) if vals("vmSize") else None,
                    "avgVmSize": mean(vals("vmSize")),
                    "peakVmSwap": max(vals("vmSwap")) if vals("vmSwap") else None,
                    "peakVmHWM": max(vals("vmHWM")) if vals("vmHWM") else None})
    cg = [r for r in rows if r.get("cgroupCurrent") is not None]
    if cg:
        peak = max(cg, key=lambda r: r["cgroupCurrent"])
        out.update({"peakCgroupCurrent": peak["cgroupCurrent"], "avgCgroupCurrent": mean(vals("cgroupCurrent")),
                    "cgroupAnonAtPeak": peak.get("cgroupAnon"), "cgroupFileAtPeak": peak.get("cgroupFile")})
    for key, col in (("peakSmapsRss", "smapsRss"), ("peakSmapsPss", "smapsPss"),
                     ("peakSmapsPssAnon", "smapsPssAnon"), ("peakSmapsPssFile", "smapsPssFile")):
        v = vals(col)
        if v:
            out[key] = max(v)
    by_phase = {}
    for r in rss:
        p = r.get("phase") or "(none)"
        by_phase[p] = max(by_phase.get(p, 0), r["vmRSS"])
    out["peakRssByPhase"] = by_phase
    return out


# ---------------------------------------------------------------------------
# GC log and NMT parsing
# ---------------------------------------------------------------------------

_UNIT = {"K": 1 << 10, "M": 1 << 20, "G": 1 << 30, "B": 1}
_GC_HEAP = re.compile(r"Pause (Young|Mixed|Full)\b.*?(\d+(?:\.\d+)?)([BKMG])->(\d+(?:\.\d+)?)([BKMG])\((\d+(?:\.\d+)?)([BKMG])\)\s+(\d+(?:\.\d+)?)ms")
_GC_PAUSE = re.compile(r"\[gc[\],].*?Pause (\w+).*?(\d+(?:\.\d+)?)ms\s*$")
_UPTIME = re.compile(r"\[(\d+(?:\.\d+)?)s\]")


def parse_gc_log(path, wall_millis):
    try:
        text = read_text(path)
    except OSError:
        return None
    after, count, full, pause_total, max_pause, committed = [], 0, 0, 0.0, 0.0, 0
    last_uptime = 0.0
    for line in text.splitlines():
        u = _UPTIME.search(line)
        if u:
            last_uptime = max(last_uptime, float(u.group(1)))
        if "[gc" not in line:
            continue
        m = _GC_HEAP.search(line)
        if m:
            count += 1
            if m.group(1) == "Full":
                full += 1
            after.append(float(m.group(4)) * _UNIT[m.group(5)])
            committed = max(committed, float(m.group(6)) * _UNIT[m.group(7)])
            ms = float(m.group(8))
            pause_total += ms
            max_pause = max(max_pause, ms)
            continue
        m = _GC_PAUSE.search(line)
        if m and m.group(1) in ("Remark", "Cleanup"):
            pause_total += float(m.group(2))
    wall = wall_millis if wall_millis else last_uptime * 1000
    return {"gcCount": count, "fullGcCount": full, "totalPauseMillis": pause_total, "maxPauseMillis": max_pause,
            "gcShareOfWall": (pause_total / wall) if wall else None,
            "peakAfterGc": int(max(after)) if after else None,
            "avgAfterGc": int(mean(after)) if after else None,
            "peakHeapCommitted": int(committed) if committed else None,
            "logUptimeMillis": int(last_uptime * 1000)}


_NMT_TOTAL = re.compile(r"^\s*Total:\s+reserved=(\d+)(KB|MB|GB)?,\s*committed=(\d+)(KB|MB|GB)?")
_NMT_CAT = re.compile(r"^\s*-\s+([A-Za-z][A-Za-z ]*?)\s+\(reserved=(\d+)(KB|MB|GB)?,\s*committed=(\d+)(KB|MB|GB)?")
_NMT_UNIT = {None: 1 << 10, "KB": 1 << 10, "MB": 1 << 20, "GB": 1 << 30}


def parse_nmt(stdout_text):
    idx = stdout_text.find("Native Memory Tracking:")
    if idx < 0:
        return None, None
    lines = [l for l in stdout_text[idx:].splitlines() if not l.startswith("CSRBENCH-")]
    block = "\n".join(lines) + "\n"
    summary = {"categories": {}}
    for l in lines:
        m = _NMT_TOTAL.match(l)
        if m and "totalReserved" not in summary:
            summary["totalReserved"] = int(m.group(1)) * _NMT_UNIT[m.group(2)]
            summary["totalCommitted"] = int(m.group(3)) * _NMT_UNIT[m.group(4)]
            continue
        m = _NMT_CAT.match(l)
        if m and m.group(1) not in summary["categories"]:
            summary["categories"][m.group(1)] = {"reserved": int(m.group(2)) * _NMT_UNIT[m.group(3)],
                                                 "committed": int(m.group(4)) * _NMT_UNIT[m.group(5)]}
    if "totalCommitted" not in summary:
        return block, None
    return block, summary


# ---------------------------------------------------------------------------
# one run
# ---------------------------------------------------------------------------

def unsafe_to_delete(snapshot, run, matrix):
    p = os.path.abspath(snapshot)
    if len([c for c in p.split(os.sep) if c]) < 3:
        return True
    others = [run.get("dataset"), matrix.get("workDir"), matrix.get("resultsDir"), os.path.expanduser("~"), os.getcwd()]
    for o in others:
        if o:
            o = os.path.abspath(o)
            if o == p or o.startswith(p + os.sep):
                return True
    return False


def compare_trees(reference, other):
    """Byte-for-byte comparison of two snapshot directories: identical when both hold the same relative file paths
    with the same contents."""
    out = {"reference": reference, "identical": False, "missing": [], "extra": [], "differing": [], "files": 0}
    if not os.path.isdir(reference) or not os.path.isdir(other):
        out["error"] = "not a directory: %s" % (reference if not os.path.isdir(reference) else other)
        return out

    def files(root):
        found = set()
        for d, _dirs, names in os.walk(root):
            for n in names:
                found.add(os.path.relpath(os.path.join(d, n), root))
        return found

    a, b = files(reference), files(other)
    out["missing"] = sorted(a - b)
    out["extra"] = sorted(b - a)
    for rel in sorted(a & b):
        if not filecmp.cmp(os.path.join(reference, rel), os.path.join(other, rel), shallow=False):
            out["differing"].append(rel)
    out["files"] = len(a & b)
    out["identical"] = not (out["missing"] or out["extra"] or out["differing"])
    return out


def resolve_relative_budget(run, results_dir):
    """Sets the builder budget of a budgetOfPeak build from the reference build's build.peakBudgetBytes: the latest
    completed hybrid build of the same dataset with an absolute budget."""
    ref = None
    for r in load_results(results_dir):  # sorted by run id, so later timestamps win
        p = r.get("params") or {}
        if (r.get("mode") == "build" and p.get("builder") == "hybrid" and p.get("budgetOfPeak") is None
                and r.get("datasetName") == run["datasetName"] and r.get("outcome") == "COMPLETED"
                and dig(r, "build.peakBudgetBytes")):
            ref = r
    if ref is None:
        raise RuntimeError("budgetOfPeak: no completed unconstrained hybrid build of %s in %s; run it first"
                           % (run["datasetName"], results_dir))
    peak = int(dig(ref, "build.peakBudgetBytes"))
    run["params"]["builderBudget"] = max(1, int(peak * float(run["params"]["budgetOfPeak"])))
    run["params"]["budgetReference"] = {"runId": ref["runId"], "peakBudgetBytes": peak}


def run_one(run, matrix, classpath, cgm, run_id, results_dir):
    e = run["entry"]
    if run["mode"] == "build" and run["params"].get("budgetOfPeak") is not None:
        resolve_relative_budget(run, results_dir)
    run_dir = os.path.join(results_dir, run_id)
    os.makedirs(run_dir, exist_ok=True)
    work_dir = os.path.join(matrix.get("workDir", os.path.join(results_dir, "work")), run_id)
    os.makedirs(work_dir, exist_ok=True)
    notes = []

    if run["mode"] == "build" and run["snapshot"] and os.path.exists(run["snapshot"]):
        if e.get("replaceSnapshot", True) and not unsafe_to_delete(run["snapshot"], run, matrix):
            notes.append("removed existing snapshot " + run["snapshot"])
            shutil.rmtree(run["snapshot"]) if os.path.isdir(run["snapshot"]) else os.remove(run["snapshot"])
        else:
            notes.append("snapshot exists and was not removed; build will likely fail")

    cache = {"requested": run["cold"] and bool(matrix.get("dropCaches", False)), "done": False, "note": None}
    if cache["requested"]:
        cache["done"], cache["note"] = drop_caches()
    elif run["mode"] == "open" and not run["cold"]:
        prewarm(run["snapshot"] or run["dataset"])

    cg_dir = cgm.create(safe(run_id)) if cgm.ok else None
    cmd = build_command(run, matrix, run_dir, work_dir, classpath, cg_dir)

    phases = PhaseState()
    result_line = []
    stdout_path = os.path.join(run_dir, "stdout.log")
    stderr_f = open(os.path.join(run_dir, "stderr.log"), "w")
    started = time.time()
    proc = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=stderr_f,
                            universal_newlines=True, errors="replace", start_new_session=True)

    def pump():
        marker = re.compile(r"^CSRBENCH-PHASE (\d+) (\S+) (begin|end)\s*$")
        with open(stdout_path, "w") as out:
            for line in proc.stdout:
                out.write(line)
                if line.startswith("CSRBENCH-"):
                    out.flush()
                    m = marker.match(line)
                    if m:
                        phases.marker(int(m.group(1)), m.group(2), m.group(3))
                    elif line.startswith("CSRBENCH-RESULT "):
                        result_line.append(line[len("CSRBENCH-RESULT "):].strip())

    reader = threading.Thread(target=pump)
    reader.daemon = True
    reader.start()
    sampler = Sampler(proc.pid, matrix.get("sampleIntervalSeconds", 1), matrix.get("smapsIntervalSeconds", 10),
                      cg_dir, phases)
    sampler.start()

    timed_out = False
    deadline = started + run["timeoutSeconds"] if run["timeoutSeconds"] else None
    try:
        while True:
            try:
                proc.wait(timeout=1)
                break
            except subprocess.TimeoutExpired:
                if deadline and time.time() > deadline:
                    timed_out = True
                    notes.append("killed after %ss timeout" % run["timeoutSeconds"])
                    try:
                        os.killpg(proc.pid, signal.SIGTERM)
                    except OSError:
                        pass
                    try:
                        proc.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        try:
                            os.killpg(proc.pid, signal.SIGKILL)
                        except OSError:
                            pass
                        if cg_dir:
                            cgm.kill(cg_dir)
                        proc.wait()
                    break
    except KeyboardInterrupt:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except OSError:
            pass
        proc.wait()
        raise
    finally:
        wall_millis = int((time.time() - started) * 1000)
        sampler.stop()
        sampler.join(10)
        reader.join(30)
        stderr_f.close()

    cgroup = {"enabled": cg_dir is not None, "path": cg_dir, "note": cgm.note if not cg_dir else "ok"}
    if cg_dir:
        events = kv_file(os.path.join(cg_dir, "memory.events"))
        stat = kv_file(os.path.join(cg_dir, "memory.stat"))
        cgroup.update({"memoryPeak": read_int(os.path.join(cg_dir, "memory.peak")),
                       "oomKill": events.get("oom_kill", 0), "finalAnon": stat.get("anon"),
                       "finalFile": stat.get("file")})
        cgm.remove(cg_dir)
    try:
        if "CSRBENCH-CGROUP-JOIN-FAILED" in read_text(os.path.join(run_dir, "stderr.log")):
            cgroup["enabled"] = False
            cgroup["note"] = "child could not join the cgroup"
    except OSError:
        pass

    write_os_series(os.path.join(run_dir, "os-series.csv"), sampler.rows)

    exit_code = proc.returncode
    result = None
    for src in (os.path.join(run_dir, "memoryrun.json"),):
        try:
            result = json.loads(read_text(src))
        except (OSError, ValueError):
            result = None
    if result is None and result_line:
        try:
            result = json.loads(result_line[-1])
        except ValueError:
            result = None
    oom_killed = (exit_code in (-9, 137) and not timed_out) or bool(cgroup.get("oomKill"))
    # Spark's uncaught-exception handler exits with 52 (SparkExitCode.OOM) on an OutOfMemoryError in its threads
    spark_oom = exit_code == 52 and run["system"] == "spark"
    if result is None:
        outcome = "TIMEOUT" if timed_out else ("OOM-KILLED" if oom_killed else ("OOM" if spark_oom else "ERROR"))
        result = {"mode": run["mode"], "system": run["system"], "dataset": run["dataset"], "query": run["query"],
                  "outcome": outcome,
                  "error": "no result from CsrMemoryRun (exit code %s)" % exit_code, "heap": None}
    elif oom_killed and result.get("outcome") == "COMPLETED":
        result["outcome"] = "OOM-KILLED"
    result.setdefault("outcome", "ERROR")

    try:
        stdout_text = read_text(stdout_path)
    except OSError:
        stdout_text = ""
    nmt_block, nmt = parse_nmt(stdout_text)
    if nmt_block:
        with open(os.path.join(run_dir, "nmt.txt"), "w") as f:
            f.write(nmt_block)

    result["runId"] = run_id
    result["params"] = run["params"]
    result["datasetName"] = run["datasetName"]
    result["cold"] = run["cold"]
    # the JVM leaves system and query empty for builds and opens; the summary keys on the matrix's values
    result["system"] = result.get("system") or run["system"]
    result["query"] = result.get("query") or run["query"]
    result["driver"] = {
        "startedAt": datetime.datetime.fromtimestamp(started, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "wallMillis": wall_millis, "exitCode": exit_code, "timedOut": timed_out, "oomKilled": oom_killed,
        "command": cmd, "notes": notes, "dropCaches": cache, "cgroup": cgroup,
        "platform": sys.platform, "os": os_stats(sampler.rows),
        "gc": parse_gc_log(os.path.join(run_dir, "gc.log"), wall_millis), "nmt": nmt,
        "phases": phases.events,
    }
    if run["mode"] == "build" and e.get("compareTo") and result.get("outcome") == "COMPLETED":
        result["identity"] = compare_trees(e["compareTo"], run["snapshot"])
    with open(os.path.join(run_dir, "result.json"), "w") as f:
        json.dump(result, f, indent=2)
    if not matrix.get("keepWorkDir", False):
        shutil.rmtree(work_dir, ignore_errors=True)
    return result


# ---------------------------------------------------------------------------
# summaries
# ---------------------------------------------------------------------------

def dig(d, path):
    for k in path.split("."):
        if not isinstance(d, dict) or d.get(k) is None:
            return None
        d = d[k]
    return d


RUN_COLUMNS = [
    ("runId", "runId"), ("mode", "mode"), ("system", "system"), ("dataset", "datasetName"), ("query", "query"),
    ("outcome", "outcome"), ("resultSize", "resultSize"), ("ownerClass", "ownerClass"), ("error", "error"),
    ("errorOwner", "errorOwner"), ("exitCode", "driver.exitCode"), ("wallMillis", "driver.wallMillis"),
    ("cold", "cold"), ("builder", "params.builder"), ("source", "params.source"),
    ("builderBudget", "params.builderBudget"), ("csrBudget", "params.csrBudget"),
    ("loadMillis", "timings.loadMillis"), ("openMillis", "timings.openMillis"),
    ("buildMillis", "timings.buildMillis"), ("coldMillis", "timings.coldMillis"),
    ("warmMedianMillis", "timings.warmMedianMillis"),
    ("resultCount", "result.count"), ("resultHash", "result.hash"),
    ("heapPeakUsed", "heap.peakUsed"), ("heapAvgUsed", "heap.avgUsed"), ("heapP95Used", "heap.p95Used"),
    ("heapPeakAfterGc", "heap.peakAfterGc"), ("heapAvgAfterGc", "heap.avgAfterGc"), ("xmx", "heap.xmx"),
    ("gcLogPeakAfterGc", "driver.gc.peakAfterGc"), ("gcLogAvgAfterGc", "driver.gc.avgAfterGc"),
    ("gcCount", "driver.gc.gcCount"), ("fullGcCount", "driver.gc.fullGcCount"),
    ("gcPauseMillis", "driver.gc.totalPauseMillis"), ("gcShareOfWall", "driver.gc.gcShareOfWall"),
    ("peakRss", "driver.os.peakRss"), ("avgRss", "driver.os.avgRss"), ("peakVmHWM", "driver.os.peakVmHWM"),
    ("rssAnonAtPeak", "driver.os.rssAnonAtPeak"), ("rssFileAtPeak", "driver.os.rssFileAtPeak"),
    ("peakVmSize", "driver.os.peakVmSize"),
    ("peakSmapsRss", "driver.os.peakSmapsRss"), ("peakSmapsPssFile", "driver.os.peakSmapsPssFile"),
    ("cgroupEnabled", "driver.cgroup.enabled"), ("cgroupMemoryPeak", "driver.cgroup.memoryPeak"),
    ("peakCgroupCurrent", "driver.os.peakCgroupCurrent"), ("avgCgroupCurrent", "driver.os.avgCgroupCurrent"),
    ("cgroupAnonAtPeak", "driver.os.cgroupAnonAtPeak"), ("cgroupFileAtPeak", "driver.os.cgroupFileAtPeak"),
    ("cgroupOomKill", "driver.cgroup.oomKill"),
    ("csrPeakBytes", "csr.peakBytes"), ("csrScratchBytes", "csr.scratchBytes"),
    ("buildPeakScratchBytes", "build.peakScratchBytes"), ("publishedBytes", "build.publishedBytes"),
    ("peakBudgetBytes", "build.peakBudgetBytes"), ("budgetOfPeak", "params.budgetOfPeak"), ("tag", "params.tag"),
    ("vertexScanSourceMillis", "buildTimers.vertex-scan_source"),
    ("vertexScanCallbackMillis", "buildTimers.vertex-scan_callback"),
    ("edgeScanSourceMillis", "buildTimers.edge-scan_source"),
    ("edgeScanCallbackMillis", "buildTimers.edge-scan_callback"),
    ("edgeScanResolveMillis", "buildTimers.edge-scan_resolve"),
    ("ioReadBytes", "io.read_bytes"), ("ioWriteBytes", "io.write_bytes"),
    ("ioCancelledWriteBytes", "io.cancelled_write_bytes"), ("ioRchar", "io.rchar"), ("ioWchar", "io.wchar"),
    ("identical", "identity.identical"), ("identityReference", "identity.reference"),
    ("nmtTotalCommitted", "driver.nmt.totalCommitted"),
]


def load_results(results_dir):
    out = []
    if not os.path.isdir(results_dir):
        return out
    for name in sorted(os.listdir(results_dir)):
        p = os.path.join(results_dir, name, "result.json")
        if os.path.exists(p):
            try:
                r = json.loads(read_text(p))
            except ValueError:
                continue
            r.setdefault("runId", name)
            if not r.get("datasetName"):
                r["datasetName"] = dataset_name(r.get("dataset"), None) if r.get("dataset") else "none"
            # build timer names contain dots, which dig() treats as path separators
            timers = dig(r, "build.timers") or {}
            r["buildTimers"] = dict((k.replace(".", "_"), v) for k, v in timers.items())
            # results written before the driver filled in system and query for builds and opens
            params = r.get("params") or {}
            if r.get("mode") == "build" and not r.get("system"):
                r["system"] = params.get("builder")
                if params.get("builderBudget") is not None:
                    r["query"] = "b%s-%s" % (human_bytes(params["builderBudget"]), params.get("source"))
            elif r.get("mode") == "open" and not r.get("query"):
                r["query"] = "cold" if r.get("cold") else "warm"
            out.append(r)
    return out


def reparse(results_dir):
    for name in sorted(os.listdir(results_dir)):
        d = os.path.join(results_dir, name)
        p = os.path.join(d, "result.json")
        if not os.path.exists(p):
            continue
        r = json.loads(read_text(p))
        drv = r.setdefault("driver", {})
        drv["gc"] = parse_gc_log(os.path.join(d, "gc.log"), drv.get("wallMillis"))
        try:
            block, nmt = parse_nmt(read_text(os.path.join(d, "stdout.log")))
        except OSError:
            block, nmt = None, None
        drv["nmt"] = nmt
        if block:
            with open(os.path.join(d, "nmt.txt"), "w") as f:
                f.write(block)
        rows = read_os_series(os.path.join(d, "os-series.csv"))
        if rows:
            drv["os"] = os_stats(rows)
        with open(p, "w") as f:
            json.dump(r, f, indent=2)


def fmt(v):
    if v is None:
        return ""
    if isinstance(v, float):
        return "%.6g" % v if abs(v) < 1e6 else "%d" % v
    return v


def summarize(results_dir):
    results = load_results(results_dir)
    # dataset name: prefer params-independent field from the run id
    with open(os.path.join(results_dir, "runs.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow([c for c, _ in RUN_COLUMNS])
        for r in results:
            w.writerow([fmt(dig(r, p)) for _, p in RUN_COLUMNS])

    latest = {}
    for r in results:  # sorted by run id, later timestamps win
        latest[(r.get("mode"), r.get("system"), r["datasetName"], r.get("query"))] = r
    agree = {}
    for (mode, system, ds, q), r in latest.items():
        if mode == "query" and r.get("outcome") == "COMPLETED" and dig(r, "result.hash") is not None:
            agree.setdefault((ds, q), {})[system] = (dig(r, "result.count"), dig(r, "result.hash"))
    cols = ["mode", "system", "dataset", "query", "outcome", "peakAfterGc", "peakRss", "peakCgroupMemory",
            "anonAtPeak", "fileAtPeak", "coldMillis", "warmMedianMillis", "scratchBytes", "resultCount",
            "resultHash", "agreement", "agreementDetail", "buildMillis", "builderBudget", "peakBudgetBytes",
            "publishedBytes", "ioReadBytes", "ioWriteBytes", "identical", "runId"]
    with open(os.path.join(results_dir, "summary.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for key in sorted(latest, key=lambda k: tuple(str(x) for x in k)):
            mode, system, ds, q = key
            r = latest[key]
            ag, detail = "", ""
            if mode == "query":
                vals = agree.get((ds, q), {})
                if len(vals) < 2 or key[1] not in vals:
                    ag = "n/a"
                elif len(set(vals.values())) == 1:
                    ag = "agree"
                else:
                    ag = "MISMATCH"
                    detail = "; ".join("%s=%s/%s" % (s, c, h) for s, (c, h) in sorted(vals.items()))
            cg_peak = dig(r, "driver.cgroup.memoryPeak")
            if cg_peak is None:
                cg_peak = dig(r, "driver.os.peakCgroupCurrent")
            anon = dig(r, "driver.os.cgroupAnonAtPeak")
            fil = dig(r, "driver.os.cgroupFileAtPeak")
            if anon is None:
                anon, fil = dig(r, "driver.os.rssAnonAtPeak"), dig(r, "driver.os.rssFileAtPeak")
            after = dig(r, "driver.gc.peakAfterGc")
            if after is None:
                after = dig(r, "heap.peakAfterGc")
            scratch = dig(r, "csr.scratchBytes")
            if scratch is None:
                scratch = dig(r, "build.peakScratchBytes")
            w.writerow([fmt(x) for x in [
                mode, system, ds, q, r.get("outcome"), after, dig(r, "driver.os.peakRss"), cg_peak, anon, fil,
                dig(r, "timings.coldMillis"), dig(r, "timings.warmMedianMillis"), scratch,
                dig(r, "result.count"), dig(r, "result.hash"), ag, detail, dig(r, "timings.buildMillis"),
                dig(r, "params.builderBudget"), dig(r, "build.peakBudgetBytes"), dig(r, "build.publishedBytes"),
                dig(r, "io.read_bytes"), dig(r, "io.write_bytes"), dig(r, "identity.identical"),
                r.get("runId")]])
    log("wrote %s and %s (%d runs)" % (os.path.join(results_dir, "runs.csv"),
                                       os.path.join(results_dir, "summary.csv"), len(results)))


# ---------------------------------------------------------------------------
# commands
# ---------------------------------------------------------------------------

def cmd_classpath(args):
    root = os.path.abspath(args.repo)
    out = os.path.abspath(args.out)
    tmp = out + ".deps"
    os.makedirs(os.path.dirname(out), exist_ok=True)
    mvn = [args.mvn, "-o", "-q", "dependency:build-classpath", "-pl", "spark-gremlin",
           "-Dmdep.includeScope=test", "-Dmdep.outputFile=" + tmp]
    log("running: " + " ".join(mvn) + "  (cwd %s)" % root)
    rc = subprocess.call(mvn, cwd=root)
    if rc != 0:
        log("mvn failed (exit %d). Offline mode needs all modules installed: mvn clean install -DskipTests" % rc)
        return rc
    deps = read_text(tmp).strip()
    os.remove(tmp)
    sg = os.path.join(root, "spark-gremlin", "target")
    with open(out, "w") as f:
        f.write(os.path.join(sg, "test-classes") + ":" + os.path.join(sg, "classes") + ":" + deps + "\n")
    log("wrote " + out)
    return 0


def cmd_run(args):
    with open(args.matrix) as f:
        matrix = json.load(f)
    try:
        runs = expand_matrix(matrix, args.only)
    except ValueError as ex:
        log("error: %s" % ex)
        return 1
    results_dir = os.path.abspath(args.results_dir or matrix.get("resultsDir", "csrbench-results"))
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S")

    if args.resume and os.path.isdir(results_dir):
        done = set()
        for name in os.listdir(results_dir):
            if os.path.exists(os.path.join(results_dir, name, "result.json")):
                done.add(re.sub(r"-\d{8}T\d{6}$", "", name))
        skipped = [r for r in runs if r["baseId"] in done]
        runs = [r for r in runs if r["baseId"] not in done]
        log("resume: skipping %d completed runs" % len(skipped))

    if args.dry_run:
        for i, r in enumerate(runs, 1):
            extra = " [cold]" if r["cold"] else ""
            log("%4d  %s-%s%s" % (i, r["baseId"], stamp, extra))
            if args.verbose:
                cmd = build_command(r, matrix, os.path.join(results_dir, "<runId>"), "<workDir>", "<classpath>", None)
                log("      " + " ".join(cmd))
        log("%d runs would execute (dry run, nothing launched); results dir %s" % (len(runs), results_dir))
        return 0

    cp_file = args.classpath_file or matrix.get("classpathFile") or DEFAULT_CLASSPATH_FILE
    if not os.path.exists(cp_file):
        log("classpath file %s not found; create it with: csrbench.py classpath" % cp_file)
        return 1
    classpath = read_text(cp_file).strip()
    os.makedirs(results_dir, exist_ok=True)
    cgm = CgroupManager(bool(matrix.get("cgroup", False)), parent=matrix.get("cgroupParent", "csrbench"))
    if matrix.get("dropCaches") and IS_LINUX and os.geteuid() != 0:
        warn("dropCaches requested but not running as root; cold runs will not be cold")
    log("%d runs, results in %s%s" % (len(runs), results_dir, "" if cgm.ok else "  (cgroups: %s)" % cgm.note))
    try:
        for i, r in enumerate(runs, 1):
            run_id = "%s-%s" % (r["baseId"], datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S"))
            t0 = time.time()
            log("[%d/%d] %s" % (i, len(runs), run_id))
            try:
                res = run_one(r, matrix, classpath, cgm, run_id, results_dir)
                log("        -> %s in %.1fs" % (res.get("outcome"), time.time() - t0))
            except KeyboardInterrupt:
                raise
            except Exception as ex:  # keep going; record the driver failure
                warn("driver error on %s: %s" % (run_id, ex))
    except KeyboardInterrupt:
        log("interrupted")
    finally:
        summarize(results_dir)
    return 0


def cmd_identical(args):
    worst = 0
    for other in args.others:
        res = compare_trees(args.reference, other)
        state = "identical" if res["identical"] else "DIFFERENT"
        log("%s: %s (%d common files)" % (other, state, res["files"]))
        for k in ("error", "missing", "extra", "differing"):
            if res.get(k):
                log("    %s: %s" % (k, res[k]))
        if not res["identical"]:
            worst = 1
    return worst


def cmd_summarize(args):
    if args.reparse:
        reparse(args.results_dir)
    summarize(args.results_dir)
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(description="CSR memory benchmark driver")
    sub = ap.add_subparsers(dest="cmd")
    sub.required = True

    p = sub.add_parser("run", help="execute a matrix")
    p.add_argument("matrix")
    p.add_argument("--dry-run", action="store_true", help="print the expanded run list; launch nothing")
    p.add_argument("-v", "--verbose", action="store_true", help="with --dry-run, print full commands")
    p.add_argument("--only", help="regex; only runs whose id (without timestamp) matches")
    p.add_argument("--resume", action="store_true", help="skip runs whose result.json already exists")
    p.add_argument("--classpath-file", help="overrides classpathFile from the matrix")
    p.add_argument("--results-dir", help="overrides resultsDir from the matrix")
    p.set_defaults(fn=cmd_run)

    p = sub.add_parser("classpath", help="build the classpath file via Maven")
    p.add_argument("--repo", default=REPO_ROOT)
    p.add_argument("--out", default=DEFAULT_CLASSPATH_FILE)
    p.add_argument("--mvn", default="mvn")
    p.set_defaults(fn=cmd_classpath)

    p = sub.add_parser("summarize", help="rebuild runs.csv and summary.csv")
    p.add_argument("results_dir")
    p.add_argument("--reparse", action="store_true",
                   help="also re-parse gc.log, NMT output and os-series.csv into result.json")
    p.set_defaults(fn=cmd_summarize)

    p = sub.add_parser("identical", help="compare snapshot directories byte for byte against a reference")
    p.add_argument("reference")
    p.add_argument("others", nargs="+")
    p.set_defaults(fn=cmd_identical)

    args = ap.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
