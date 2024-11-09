/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph
import org.apache.tinkerpop.gremlin.process.traversal.translator.JavascriptTranslator
import org.apache.tinkerpop.gremlin.jsr223.ScriptCustomizer
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine
import org.apache.tinkerpop.gremlin.groovy.jsr223.ast.AmbiguousMethodASTTransformation
import org.apache.tinkerpop.gremlin.groovy.jsr223.ast.VarAsBindingASTTransformation
import org.apache.tinkerpop.gremlin.groovy.jsr223.ast.RepeatASTTransformationCustomizer
import org.apache.tinkerpop.gremlin.groovy.jsr223.GroovyCustomizer
import org.codehaus.groovy.control.customizers.CompilationCustomizer
import org.apache.tinkerpop.gremlin.language.corpus.FeatureReader

import javax.script.SimpleBindings
import java.nio.file.Paths

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal

// getting an exception like:
// > InvocationTargetException: javax.script.ScriptException: groovy.lang.MissingMethodException: No signature of
// > method: org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal.mergeE() is applicable for
// > argument types: (String) values: [4ffdea36-4a0e-4681-acba-e76875d1b25b]
// usually means bindings are not being extracted properly by VarAsBindingASTTransformation which typically happens
// when a step is taking an argument that cannot properly resolve to the type required by the step itself. there are
// special cases in that VarAsBindingASTTransformation class which might need to be adjusted. Editing the
// GremlinGroovyScriptEngineTest#shouldProduceBindingsForVars() with the failing step and argument can typically make
// this issue relatively easy to debug and enforce.
//
// getting an exception like:
// > Ambiguous method overloading for method org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource#mergeV.
// likely requires changes to the AmbiguousMethodASTTransformation which forces a call to a particular method overload
// and usually relates to use of null where the type isn't clear

// file is overwritten on each generation
radishGremlinFile = new File("${projectBaseDir}/gremlin-javascript/src/main/javascript/gremlin-javascript/test/cucumber/gremlin.js")

// assumes globally unique scenario names for keys with list of Gremlin traversals as they appear
gremlins = FeatureReader.parseGrouped(Paths.get("${projectBaseDir}", "gremlin-test", "src", "main", "resources", "org", "apache", "tinkerpop", "gremlin", "test", "features").toString())

gremlinGroovyScriptEngine = new GremlinGroovyScriptEngine(
        (GroovyCustomizer) { -> new RepeatASTTransformationCustomizer(new AmbiguousMethodASTTransformation()) },
        (GroovyCustomizer) { -> new RepeatASTTransformationCustomizer(new VarAsBindingASTTransformation()) }
)

translator = JavascriptTranslator.of('g')
g = traversal().withEmbedded(EmptyGraph.instance())
bindings = new SimpleBindings()
bindings.put('g', g)

radishGremlinFile.withWriter('UTF-8') { Writer writer ->
    writer.writeLine('/*\n' +
            '* Licensed to the Apache Software Foundation (ASF) under one\n' +
            '* or more contributor license agreements.  See the NOTICE file\n' +
            '* distributed with this work for additional information\n' +
            '* regarding copyright ownership.  The ASF licenses this file\n' +
            '* to you under the Apache License, Version 2.0 (the\n' +
            '* "License"); you may not use this file except in compliance\n' +
            '* with the License.  You may obtain a copy of the License at\n' +
            '* \n' +
            '* http://www.apache.org/licenses/LICENSE-2.0\n' +
            '* \n' +
            '* Unless required by applicable law or agreed to in writing,\n' +
            '* software distributed under the License is distributed on an\n' +
            '* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY\n' +
            '* KIND, either express or implied.  See the License for the\n' +
            '* specific language governing permissions and limitations\n' +
            '* under the License.\n' +
            '*/\n')

    writer.writeLine("\n\n//********************************************************************************")
    writer.writeLine("//* Do NOT edit this file directly - generated by build/generate.groovy")
    writer.writeLine("//********************************************************************************\n\n")

    writer.writeLine(
                    'const graphTraversalModule = require(\'../../lib/process/graph-traversal\');\n' +
                    'const traversalModule = require(\'../../lib/process/traversal\');\n' +
                    'const { TraversalStrategies, VertexProgramStrategy, OptionsStrategy, PartitionStrategy, ReadOnlyStrategy, SeedStrategy, SubgraphStrategy, ProductiveByStrategy } = require(\'../../lib/process/traversal-strategy\');\n' +
                    'const __ = graphTraversalModule.statics;\n' +
                    'const Barrier = traversalModule.barrier\n' +
                    'const Cardinality = traversalModule.cardinality\n' +
                    'const CardinalityValue = graphTraversalModule.CardinalityValue;\n' +
                    'const Column = traversalModule.column\n' +
                    'const Direction = {\n' +
                    '    BOTH: traversalModule.direction.both,\n' +
                    '    IN: traversalModule.direction.in,\n' +
                    '    OUT: traversalModule.direction.out,\n' +
                    '    from_: traversalModule.direction.out,\n' +
                    '    to: traversalModule.direction.in\n' +
                    '};\n' +
                    'const DT = traversalModule.dt;\n' +
                    'const Merge = traversalModule.merge;\n' +
                    'const P = traversalModule.P;\n' +
                    'const Pick = traversalModule.pick\n' +
                    'const Pop = traversalModule.pop\n' +
                    'const Order = traversalModule.order\n' +
                    'const Operator = traversalModule.operator\n' +
                    'const Scope = traversalModule.scope\n' +
                    'const T = traversalModule.t\n' +
                    'const TextP = traversalModule.TextP\n' +
                    'const WithOptions = traversalModule.withOptions\n'
    )

    // Groovy can't process certain null oriented calls because it gets confused with the right overload to call
    // at runtime. using this approach for now as these are the only such situations encountered so far. a better
    // solution may become necessary as testing of nulls expands.
    def staticTranslate = [:]
    // SAMPLE: g_injectXnull_nullX: "    g_injectXnull_nullX: [function({g}) { return g.inject(null,null) }], ",

    writer.writeLine('const gremlins = {')
    gremlins.each { k,v ->
        if (staticTranslate.containsKey(k)) {
            writer.writeLine(staticTranslate[k])
        } else {
            writer.write("    ")
            writer.write(k)
            writer.write(": [")
            def collected = v.collect {
                def t = gremlinGroovyScriptEngine.eval(it, bindings)
                [t, t.bytecode.bindings.keySet()]
            }
            def uniqueBindings = collected.collect { it[1] }.flatten().unique()
            def gremlinItty = collected.iterator()
            while (gremlinItty.hasNext()) {
                def t = gremlinItty.next()[0]
                writer.write("function({g")
                if (!uniqueBindings.isEmpty()) {
                    writer.write(", ")
                    writer.write(uniqueBindings.join(", "))
                }
                writer.write("}) { return ")
                writer.write(translator.translate(t.bytecode).script)
                writer.write(" }")
                if (gremlinItty.hasNext()) writer.write(', ')
            }
            writer.writeLine('], ')
        }
    }
    writer.writeLine('}\n')

    writer.writeLine('exports.gremlin = gremlins')
}


