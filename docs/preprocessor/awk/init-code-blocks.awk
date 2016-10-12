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
    print "import static javax.script.ScriptContext.*"
    print "import org.apache.tinkerpop.gremlin.python.jsr223.JythonTranslator"
    print "jython = new org.apache.tinkerpop.gremlin.python.jsr223.GremlinJythonScriptEngine()"
    print "jython.eval('import os')"
    print "jython.eval('os.chdir(\"" TP_HOME "\")')"
    print "jython.eval('import sys')"
    print "jython.eval('sys.path.append(\"" PYTHONPATH "\")')"
    print "jython.eval('sys.path.append(\"" TP_HOME "/gremlin-python/target/test-classes/Lib\")')"
    print "jython.eval('from gremlin_python import statics')"
    print "jython.eval('from gremlin_python.process.traversal import *')"
    print "jython.eval('from gremlin_python.process.strategies import *')"
    print "jython.eval('from gremlin_python.structure.graph import Graph')"
    print "jython.eval('from gremlin_python.structure.io.graphson import GraphSONWriter')"
    print "jython.eval('from gremlin_python.structure.io.graphson import GraphSONReader')"
    # print "jython.eval('from gremlin_python.structure.io.graphson import serializers')"
    # print "jython.eval('from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection')"
    print "jython.eval('statics.load_statics(globals())')"
    print "jythonBindings = jython.createBindings()"
    print "jythonBindings.put('g', jython.eval('Graph().traversal()'))"
    print "jythonBindings.put('h', g)"
    print "jython.getContext().setBindings(jythonBindings, GLOBAL_SCOPE)"
    print "groovy = new org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine()"
    print "groovyBindings = groovy.createBindings()"
    print "groovyBindings.put('g', g)"
    print "groovyBindings.put('TinkerGraphComputer', TinkerGraphComputer)"
    print "groovy.getContext().setBindings(groovyBindings, GLOBAL_SCOPE)"
    print "def processTraversal(t, jython, groovy) {"
    print "  jython.getContext().getBindings(GLOBAL_SCOPE).put('j', jython.eval(t.replace('.toList()','')))"
    print "  if(jython.eval('isinstance(j, Traversal)')) {"
    print "    mapper = GraphSONMapper.build().version(GraphSONVersion.V2_0).create().createMapper()"
    print "    bytecode = mapper.readValue(jython.eval('GraphSONWriter().writeObject(j)').toString(), Bytecode.class)"
    print "    language = BytecodeHelper.getLambdaLanguage(bytecode).orElse('gremlin-groovy')"
    print "    result = language.equals('gremlin-groovy') ? groovy.eval(GroovyTranslator.of(\"g\").translate(bytecode) + '.toList()').toString() : jython.eval(JythonTranslator.of(\"h\").translate(bytecode) + '.toList()').toString()"
    print "    jython.getContext().getBindings(GLOBAL_SCOPE).put('json', mapper.writeValueAsString(result))"
    print "    return jython.eval('GraphSONReader().readObject(json)').toString()"
    print "  } else {"
    print "    j = jython.getContext().getBindings(GLOBAL_SCOPE).get('j')"
    print "    return null == j ? 'null' : j.toString()"
    print "  }"
    print "}"
  }
  print "'-IGNORE'"
}

!/^pb\([0-9]*\); '\[gremlin-/ {
  if (delimiter == 2 && !($0 ~ /^pb\([0-9]*\); '----'/)) {
    switch (lang) {
      case "python":
        print "processTraversal(\"\"\"" gensub("\\('id',([0-9]+)\\)", "\\1", "g") "\"\"\", jython, groovy)"
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
