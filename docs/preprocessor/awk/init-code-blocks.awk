# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

#
# @author Daniel Kuppitz (http://gremlin.guru)
#
function capitalize(string) {
  return toupper(substr(string, 1, 1)) substr(string, 2)
}

BEGIN {
  delimiter = 0
}

/^pb\([0-9]*\); '\[gremlin-/ {
  delimiter = 1
  split($0, a, "-")
  b = gensub(/]'/, "", "g", a[2])
  split(b, c, ",")
  split(a[1], d, ";")
  lang = c[1]
  graph = c[2]
  print d[1] "; '[source," lang "]'"
  print "'+EVALUATED'"
  print "'+IGNORE'"
  if (graph) {
    print "graph = TinkerFactory.create" capitalize(graph) "()"
  } else {
    print "graph = TinkerGraph.open()"
  }
  print "g = graph.traversal()"
  print "marko = g.V().has('name', 'marko').tryNext().orElse(null)"
  print "f = new File('/tmp/neo4j')"
  print "if (f.exists()) f.deleteDir()"
  print "f = new File('/tmp/tinkergraph.kryo')"
  print "if (f.exists()) f.deleteDir()"
  print ":set max-iteration 100"
  if (lang == "python") {
    print "import org.apache.tinkerpop.gremlin.python.jsr223.PythonTranslator"
    print "import javax.script.ScriptEngineManager"
    print "import javax.script.SimpleBindings"
    print "def downloadFile(String url, final String localPath) {"
    print "  def f = new File(localPath)"
    print "  while (!f.exists() && url) {"
    print "    url.toURL().openConnection().with { def conn ->"
    print "      conn.instanceFollowRedirects = false"
    print "      url = conn.getHeaderField('Location')"
    print "      if (!url) {"
    print "        f.withOutputStream { def output ->"
    print "          conn.inputStream.with { def input ->"
    print "            output << input"
    print "            input.close()"
    print "          }"
    print "        }"
    print "      }"
    print "    }"
    print "  }"
    print "  return f"
    print "}"
    pythonVersion = "2.7.0"
    print "pathToLocalJar = System.getProperty('java.io.tmpdir') + File.separator + 'jython-standalone-" pythonVersion ".jar'"
    print "jarDownloadUrl = 'http://search.maven.org/remotecontent?filepath=org/python/jython-standalone/" pythonVersion "/jython-standalone-" pythonVersion ".jar'"
    print "loader = new URLClassLoader(downloadFile(jarDownloadUrl, pathToLocalJar).toURI().toURL())"
    print "jython = new ScriptEngineManager(loader).getEngineByName('jython')"
    print "jython.eval('import sys')"
    print "jython.eval('sys.path.append(\"" PYTHONPATH "\")')"
    print "jython.eval('sys.path.append(\"" TP_HOME "/gremlin-variant/src/main/jython/gremlin_python\")')"
    print "jython.eval('sys.path.append(\"" TP_HOME "/gremlin-variant/src/main/jython/gremlin_python.driver\")')"
    print "jython.eval('sys.path.append(\"" TP_HOME "/gremlin-variant/src/main/jython/gremlin_rest_driver\")')"
    print "jython.eval('from gremlin_python import *')"
    #print "jython.eval('from gremlin_rest_driver import RESTRemoteConnection')"
    print "jython.eval('from groovy_translator import GroovyTranslator')"
    print "jython.eval('for k in statics:\\n  globals()[k] = statics[k]')"
    print "jythonBindings = new SimpleBindings()"
    print "jythonBindings.put('g', jython.eval('PythonGraphTraversalSource(GroovyTranslator(\"g\"))'))"
    print "jython.getContext().setBindings(jythonBindings, javax.script.ScriptContext.GLOBAL_SCOPE)"
    print "groovyBindings = new SimpleBindings()"
    print "groovyBindings.put('g', g)"
    print "groovyBindings.put('TinkerGraphComputer', TinkerGraphComputer)"
    print "groovy = new org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine()"
    print "groovy.getContext().setBindings(groovyBindings, javax.script.ScriptContext.GLOBAL_SCOPE)"
  }
  print "'-IGNORE'"
}

!/^pb\([0-9]*\); '\[gremlin-/ {
  if (delimiter == 2 && !($0 ~ /^pb\([0-9]*\); '----'/)) {
    switch (lang) {
      case "python":
        print "groovy.eval jython.eval(\"\"\"" gensub("\\('id',([0-9]+)\\)", "\\1", "g") "\"\"\").toString()"
        break
      default:
        print
        break
    }
  } else print
}

/^pb\([0-9]*\); '----'/ {
  if (delimiter == 1) delimiter = 2
  else if (delimiter == 2) {
    print "'-EVALUATED'"
    delimiter = 0
  }
}
