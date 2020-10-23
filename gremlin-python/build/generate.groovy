import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph
import org.apache.tinkerpop.gremlin.process.traversal.translator.PythonTranslator
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine
import org.apache.tinkerpop.gremlin.groovy.jsr223.ast.VarAsBindingASTTransformation
import org.apache.tinkerpop.gremlin.groovy.jsr223.ast.RepeatASTTransformationCustomizer
import org.apache.tinkerpop.gremlin.groovy.jsr223.GroovyCustomizer
import org.apache.commons.text.StringEscapeUtils
import org.codehaus.groovy.control.customizers.CompilationCustomizer

import java.io.File
import javax.script.SimpleBindings
import groovy.io.FileType

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal

// file is overwritten on each generation
radishGremlinFile = new File('/home/smallette/git/apache/tinkerpop/gremlin-python/src/main/python/radish/gremlin.py')

// assumes globally unique scenario names for keys with list of Gremlin traversals as they appear
gremlins = [:].withDefault{[]}

gremlinGroovyScriptEngine = new GremlinGroovyScriptEngine(new GroovyCustomizer() {
    public CompilationCustomizer create() {
        return new RepeatASTTransformationCustomizer(new VarAsBindingASTTransformation())
    }
})
pythonTranslator = PythonTranslator.of('g')
g = traversal().withGraph(EmptyGraph.instance())
bindings = new SimpleBindings()
bindings.put('g', g)

featureDir = new File('/home/smallette/git/apache/tinkerpop/gremlin-test/features')
featureDir.eachFileRecurse (FileType.FILES) { file ->
    // only process gherkin files with .feature extension
    if (file.name.endsWith('.feature')) {
        def currentGremlin = ''
        def openTriples = false
        def skipIgnored = false
        def scenarioName = ''
        file.withReader { Reader reader ->
            reader.eachLine { String line ->
                def cleanLine = line.trim();
                if (cleanLine.startsWith('Scenario:')) {
                    scenarioName = cleanLine.split(':')[1].trim()
                    skipIgnored = false
                } else if (cleanLine.startsWith('Then nothing should happen because')) {
                    skipIgnored = true
                } else if (cleanLine.startsWith('And the graph should return')) {
                    gremlins[scenarioName] << StringEscapeUtils.unescapeJava(cleanLine.substring(cleanLine.indexOf("\"") + 1, cleanLine.lastIndexOf("\"")))
                } else if (cleanLine.startsWith("\"\"\"")) {
                    openTriples = !openTriples
                    if (!skipIgnored && !openTriples) {
                        gremlins[scenarioName] << currentGremlin
                        currentGremlin = ""
                        println "added: " + gremlins[scenarioName]
                    }
                } else if (openTriples && !skipIgnored) {
                    currentGremlin += cleanLine
                }
            }
        }
    }
}

radishGremlinFile.withWriter('UTF-8') { Writer writer ->
    writer.writeLine('#\n' +
            '# Licensed to the Apache Software Foundation (ASF) under one\n' +
            '# or more contributor license agreements.  See the NOTICE file\n' +
            '# distributed with this work for additional information\n' +
            '# regarding copyright ownership.  The ASF licenses this file\n' +
            '# to you under the Apache License, Version 2.0 (the\n' +
            '# "License"); you may not use this file except in compliance\n' +
            '# with the License.  You may obtain a copy of the License at\n' +
            '# \n' +
            '# http://www.apache.org/licenses/LICENSE-2.0\n' +
            '# \n' +
            '# Unless required by applicable law or agreed to in writing,\n' +
            '# software distributed under the License is distributed on an\n' +
            '# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY\n' +
            '# KIND, either express or implied.  See the License for the\n' +
            '# specific language governing permissions and limitations\n' +
            '# under the License.\n' +
            '#\n')
    writer.writeLine(
                    'from radish import world\n' +
                    'from gremlin_python.process.anonymous_traversal import traversal\n' +
                    'from gremlin_python.process.traversal import TraversalStrategy\n' +
                    'from gremlin_python.process.graph_traversal import __\n' +
                    'from gremlin_python.structure.graph import Graph\n' +
                    'from gremlin_python.process.traversal import Barrier, Cardinality, P, TextP, Pop, Scope, Column, Order, Direction, T, Pick, Operator, IO, WithOptions\n')
    writer.writeLine('world.gremlins = {')
    gremlins.each { k,v ->
        writer.write("    '")
        writer.write(k)
        writer.write("':[")
        def collected = v.collect{
            def t = gremlinGroovyScriptEngine.eval(it, bindings)
            [t, t.bytecode.bindings.keySet()]
        }
        def uniqueBindings = collected.collect{it[1]}.flatten().unique()
        def gremlinItty = collected.iterator()
        while (gremlinItty.hasNext()) {
            def t = gremlinItty.next()[0]
            writer.write("(lambda g")
            if (!uniqueBindings.isEmpty()) {
                writer.write(", ")
                writer.write(uniqueBindings.join("=None,"))
                writer.write("=None")
            }
            writer.write(":")
            writer.write(pythonTranslator.translate(t.bytecode).script)
            writer.write(")")
            if (gremlinItty.hasNext()) writer.write(', ')
        }
        writer.writeLine('], ')
    }
    writer.writeLine('}')
}


