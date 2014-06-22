package com.tinkerpop.gremlin.groovy

import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine
import com.tinkerpop.gremlin.groovy.loaders.GraphLoader
import com.tinkerpop.gremlin.groovy.loaders.ObjectLoader
import com.tinkerpop.gremlin.groovy.loaders.StepLoader
import com.tinkerpop.gremlin.process.Step
import com.tinkerpop.gremlin.process.Tokens
import com.tinkerpop.gremlin.process.graph.GraphTraversal
import com.tinkerpop.gremlin.structure.Edge
import com.tinkerpop.gremlin.structure.Graph
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GremlinLoader {

    private static final Set<String> steps = new HashSet<String>()
    private static final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine()

    public static void load() {

        [GraphTraversal, Graph, Vertex, Edge].forEach {
            it.getMethods().findAll {
                it.getReturnType().equals(GraphTraversal.class)
            }.each {
                addStep(it.getName())
            }
        }

        GraphLoader.load()
        ObjectLoader.load()
        StepLoader.load()

        try {
            //SailGraphLoader.load()
        } catch (Throwable e) {
            // this means that SailGraph was not in the dependency
            // that is ok
        }
    }

    // todo: clean this up...how much of this is needed?

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

    public static String version() {
        return Tokens.VERSION
    }

    public static String language() {
        return "gremlin-groovy"
    }
}
