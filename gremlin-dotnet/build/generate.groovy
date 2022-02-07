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
import org.apache.tinkerpop.gremlin.process.traversal.translator.DotNetTranslator
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
radishGremlinFile = new File("${projectBaseDir}/gremlin-dotnet/test/Gremlin.Net.IntegrationTest/Gherkin/Gremlin.cs")

// assumes globally unique scenario names for keys with list of Gremlin traversals as they appear
gremlins = FeatureReader.parseGrouped(Paths.get("${projectBaseDir}", "gremlin-test", "features").toString())

gremlinGroovyScriptEngine = new GremlinGroovyScriptEngine(new GroovyCustomizer() {
    public CompilationCustomizer create() {
        return new RepeatASTTransformationCustomizer(new AmbiguousMethodASTTransformation())
    }
}, new GroovyCustomizer() {
    public CompilationCustomizer create() {
        return new RepeatASTTransformationCustomizer(new VarAsBindingASTTransformation())
    }
})
translator = DotNetTranslator.of('g')
g = traversal().withEmbedded(EmptyGraph.instance())
bindings = new SimpleBindings()
bindings.put('g', g)

radishGremlinFile.withWriter('UTF-8') { Writer writer ->
    writer.writeLine('#region License\n' +
            '\n' +
            '/*\n' +
            ' * Licensed to the Apache Software Foundation (ASF) under one\n' +
            ' * or more contributor license agreements.  See the NOTICE file\n' +
            ' * distributed with this work for additional information\n' +
            ' * regarding copyright ownership.  The ASF licenses this file\n' +
            ' * to you under the Apache License, Version 2.0 (the\n' +
            ' * "License"); you may not use this file except in compliance\n' +
            ' * with the License.  You may obtain a copy of the License at\n' +
            ' *\n' +
            ' *     http://www.apache.org/licenses/LICENSE-2.0\n' +
            ' *\n' +
            ' * Unless required by applicable law or agreed to in writing,\n' +
            ' * software distributed under the License is distributed on an\n' +
            ' * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY\n' +
            ' * KIND, either express or implied.  See the License for the\n' +
            ' * specific language governing permissions and limitations\n' +
            ' * under the License.\n' +
            ' */\n' +
            '\n' +
            '#endregion\n')

    writer.writeLine("\n\n//********************************************************************************")
    writer.writeLine("//* Do NOT edit this file directly - generated by build/generate.groovy")
    writer.writeLine("//********************************************************************************\n\n")

    writer.writeLine('using System;\n' +
                     'using System.Collections.Generic;\n' +
                     'using Gremlin.Net.Structure;\n' +
                     'using Gremlin.Net.Process.Traversal;\n' +
                     'using Gremlin.Net.Process.Traversal.Strategy.Optimization;\n' +
                     'using Gremlin.Net.Process.Traversal.Strategy.Decoration;\n')
    writer.writeLine('namespace Gremlin.Net.IntegrationTest.Gherkin\n' +
            '{\n' +
            '    public class Gremlin\n' +
            '    {\n' +
            '        public static void InstantiateTranslationsForTestRun()\n' +
            '        {\n' +
            '            // We need to copy the fixed translations as we remove translations from the list after using them\n' +
            '            // so we can enumerate through the translations while evaluating a scenario.\n' +
            '            _translationsForTestRun =\n' +
            '                new Dictionary<string, List<Func<GraphTraversalSource, IDictionary<string, object>, ITraversal>>>(\n' +
            '                    FixedTranslations.Count);\n' +
            '            foreach (var (traversal, translations) in FixedTranslations)\n' +
            '            {\n' +
            '                _translationsForTestRun.Add(traversal,\n' +
            '                    new List<Func<GraphTraversalSource, IDictionary<string, object>, ITraversal>>(translations));\n' +
            '            }\n' +
            '        }\n')
    writer.writeLine(
            '        private static IDictionary<string, List<Func<GraphTraversalSource, IDictionary<string, object>, ITraversal>>>\n' +
            '            _translationsForTestRun;\n')
    writer.writeLine(
            '        private static readonly IDictionary<string, List<Func<GraphTraversalSource, IDictionary<string, object>,ITraversal>>> FixedTranslations = \n' +
            '            new Dictionary<string, List<Func<GraphTraversalSource, IDictionary<string, object>, ITraversal>>>\n' +
            '            {')

    // Groovy can't process certain null oriented calls because it gets confused with the right overload to call
    // at runtime. using this approach for now as these are the only such situations encountered so far. a better
    // solution may become necessary as testing of nulls expands.
    def staticTranslate = [
            g_injectXnull_nullX: "               {\"g_injectXnull_nullX\", new List<Func<GraphTraversalSource, IDictionary<string, object>, ITraversal>> {(g,p) =>g.Inject<object>(null,null)}}, ",
            g_V_hasIdXnullX: "               {\"g_V_hasIdXnullX\", new List<Func<GraphTraversalSource, IDictionary<string, object>, ITraversal>> {(g,p) =>g.V().HasId(null)}}, ",
            g_V_hasIdX2_nullX: "               {\"g_V_hasIdX2_nullX\", new List<Func<GraphTraversalSource, IDictionary<string, object>, ITraversal>> {(g,p) =>g.V().HasId(p[\"vid2\"],null)}}, ",
            g_V_hasIdX2AsString_nullX: "               {\"g_V_hasIdX2AsString_nullX\", new List<Func<GraphTraversalSource, IDictionary<string, object>, ITraversal>> {(g,p) =>g.V().HasId(p[\"vid2\"],null)}}, ",
            g_VX1X_valuesXageX_injectXnull_nullX: "               {\"g_VX1X_valuesXageX_injectXnull_nullX\", new List<Func<GraphTraversalSource, IDictionary<string, object>, ITraversal>> {(g,p) =>g.V(p[\"xx1\"]).Values<object>(\"age\").Inject(null,null)}}, "
    ]

    gremlins.each { k,v ->
        if (staticTranslate.containsKey(k)) {
            writer.writeLine(staticTranslate[k])
        } else {
            writer.write("               {\"")
            writer.write(k)
            writer.write("\", new List<Func<GraphTraversalSource, IDictionary<string, object>, ITraversal>> {")
            def collected = v.collect {
                def t = gremlinGroovyScriptEngine.eval(it, bindings)
                [t, t.bytecode.bindings.keySet()]
            }

            def gremlinItty = collected.iterator()
            while (gremlinItty.hasNext()) {
                def t = gremlinItty.next()[0]
                writer.write("(g,p) =>")
                writer.write(translator.translate(t.bytecode).script.
                        replaceAll("xx([0-9]+)", "p[\"xx\$1\"]").
                        replaceAll("v([0-9]+)", "(Vertex) p[\"v\$1\"]").
                        replaceAll("vid([0-9]+)", "p[\"vid\$1\"]").
                        replaceAll("e([0-9]+)", "p[\"e\$1\"]").
                        replaceAll("eid([0-9]+)", "p[\"eid\$1\"]").
                        replaceAll("l([0-9]+)", "(IFunction) p[\"l\$1\"]").
                        replaceAll("pred([0-9]+)", "(IPredicate) p[\"pred\$1\"]").
                        replaceAll("c([0-9]+)", "(IComparator) p[\"c\$1\"]"))
                if (gremlinItty.hasNext())
                    writer.write(', ')
                else
                    writer.write("}")
            }
            writer.writeLine('}, ')
        }
    }
    writer.writeLine('            };\n')

    writer.writeLine(
            '        public static ITraversal UseTraversal(string scenarioName, GraphTraversalSource g, IDictionary<string, object> parameters)\n' +
            '        {\n' +
            '            List<Func<GraphTraversalSource, IDictionary<string, object>, ITraversal>> list = _translationsForTestRun[scenarioName];\n' +
            '            Func<GraphTraversalSource, IDictionary<string, object>, ITraversal> f = list[0];\n' +
            '            list.RemoveAt(0);\n' +
            '            return f.Invoke(g, parameters);\n' +
            '        }\n' +
            '    }\n' +
            '}\n')
}


