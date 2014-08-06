package com.tinkerpop.gremlin.groovy.loaders

import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine
import com.tinkerpop.gremlin.process.Step
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.GraphTraversal
import com.tinkerpop.gremlin.structure.Edge
import com.tinkerpop.gremlin.structure.Element
import com.tinkerpop.gremlin.structure.Graph
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class GremlinLoader {

    private static final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine()
    private static final Set<String> steps = new HashSet<String>()
    static {
        [GraphTraversal, Graph, Vertex, Edge, Element].forEach {
            it.getMethods().findAll {
                Traversal.class.isAssignableFrom(it.getReturnType());
            }.each {
                addStep(it.getName())
            }
        }
    }

    public static void load() {
        GraphLoader.load()
        ObjectLoader.load()
        StepLoader.load()
    }

    public static Step compile(final String script) {
        return (Step) engine.eval(script, engine.createBindings())
    }

    public static void addStep(final String stepName) {
        steps.add(stepName)
    }

    public static boolean isStep(final String stepName) {
        return steps.contains(stepName)
    }

    public static Set<String> getStepNames() {
        return new HashSet(steps)
    }
}
