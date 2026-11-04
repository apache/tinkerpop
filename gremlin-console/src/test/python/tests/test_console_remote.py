'''
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
'''

"""
Sending traversals from the console to a running Gremlin Server.

A Gremlin Server from the local build (gremlin-server/target) is started once for this module, bound to localhost on
a free port. The tests are skipped when that build is not available (for example inside the Docker test image, which
only mounts the console distribution).
"""

import glob
import os
import shutil
import socket
import subprocess
import tempfile
import time

import pytest

from .console_helpers import PROMPT, STACK_TRACE_PROMPT, exit_console, results

SERVER_HOME_PATTERN = os.path.abspath(os.path.join("..", "..", "..", "..", "gremlin-server", "target",
                                                   "apache-tinkerpop-gremlin-server-*-standalone"))


def _free_port():
    with socket.socket() as s:
        s.bind(("localhost", 0))
        return s.getsockname()[1]


def _wait_for_port(port, process, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            return False
        try:
            with socket.create_connection(("localhost", port), timeout=1):
                return True
        except OSError:
            time.sleep(1)
    return False


@pytest.fixture(scope="module")
def server():
    homes = sorted(glob.glob(SERVER_HOME_PATTERN))
    if not homes:
        pytest.skip("no Gremlin Server build found at " + SERVER_HOME_PATTERN)
    home = homes[-1]
    port = _free_port()
    work = tempfile.mkdtemp(prefix="gremlin-server-test-")

    # the modern-graph configuration, on a free port and only reachable from this machine
    with open(os.path.join(home, "conf", "gremlin-server-modern.yaml")) as f:
        config = f.read()
    assert "host: localhost" in config
    config = config.replace("port: 8182", "port: %d" % port) \
                   .replace("conf/tinkergraph-empty.properties",
                            os.path.join(home, "conf", "tinkergraph-empty.properties"))
    server_yaml = os.path.join(work, "server.yaml")
    with open(server_yaml, "w") as f:
        f.write(config)

    # matching client files for the console
    remote_yaml = os.path.join(work, "remote.yaml")
    with open(remote_yaml, "w") as f:
        # same format as the conf/remote.yaml shipped with the console
        f.write("hosts: [localhost]\nport: %d\nresponseSerializer: { className: "
                "org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV4 }\n" % port)
    remote_properties = os.path.join(work, "remote-graph.properties")
    with open(remote_properties, "w") as f:
        f.write("gremlin.remote.remoteConnectionClass=org.apache.tinkerpop.gremlin.driver.remote."
                "DriverRemoteConnection\n"
                "gremlin.remote.driver.clusterFile=%s\n"
                "gremlin.remote.driver.sourceName=g\n" % remote_yaml)

    log = open(os.path.join(work, "server.log"), "w")
    env = dict(os.environ)
    env.pop("JAVA_OPTIONS", None)
    process = subprocess.Popen(["bash", os.path.join(home, "bin", "gremlin-server.sh"), server_yaml],
                               cwd=home, stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, env=env)
    try:
        if not _wait_for_port(port, process):
            log.flush()
            with open(log.name) as f:
                pytest.fail("Gremlin Server did not start:\n" + f.read()[-3000:])
        # the port can open before the server is ready to answer, so wait for its "started" log line too
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            log.flush()
            with open(log.name) as f:
                if "Channel started at port" in f.read():
                    break
            time.sleep(1)
        time.sleep(2)
        yield {"port": port, "properties": remote_properties}
    finally:
        process.terminate()
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()
        log.close()
        shutil.rmtree(work, ignore_errors=True)


def test_remote_traversal_source(console, server):
    child, _ = console.start("-C")
    assert results(child, "g = traversal().with('%s')" % server["properties"]) == ["==>null"]
    output = results(child, "g")
    assert len(output) == 1 and output[0].startswith("==>graphtraversalsource["), output
    assert results(child, "g.V().count()") == ["==>6"]
    assert results(child, "g.V().has('name', 'marko').out('knows').values('name').order()") == \
        ["==>josh", "==>vadas"]
    assert send_close(child, "g.close()")
    assert exit_console(child) == 0


def test_remote_driver_connection(console, server):
    child, _ = console.start("-C")
    results(child, "g = traversal().with(DriverRemoteConnection.using('localhost', %d, 'g'))" % server["port"])
    assert results(child, "g.V().hasLabel('software').count()") == ["==>2"]
    assert send_close(child, "g.close()")
    assert exit_console(child) == 0


def test_remote_client_submit(console, server):
    child, _ = console.start("-C")
    results(child, "cluster = Cluster.build('localhost').port(%d).create()" % server["port"])
    results(child, "client = cluster.connect()")
    assert results(child, "client.submit('g.V().count()').one().getLong()") == ["==>6"]
    results(child, "client.close()")
    results(child, "cluster.close()")
    assert exit_console(child) == 0


def send_close(child, line):
    """Closes a remote source; returns True when the console is back at its prompt without an error."""
    child.sendline(line)
    return child.expect_exact([PROMPT, STACK_TRACE_PROMPT]) == 0
