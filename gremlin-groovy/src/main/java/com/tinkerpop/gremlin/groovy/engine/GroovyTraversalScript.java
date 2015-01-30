package com.tinkerpop.gremlin.groovy.engine;

import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngineFactory;
import com.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.ComputerResult;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalScript;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.process.computer.traversal.step.map.ComputerResultStep;
import com.tinkerpop.gremlin.process.graph.traversal.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.traversal.DefaultGraphTraversal;
import com.tinkerpop.gremlin.structure.Graph;
import org.apache.commons.configuration.Configuration;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroovyTraversalScript<S, E> implements TraversalScript<S, E> {

    private static final String ENGINE_NAME = new GremlinGroovyScriptEngineFactory().getEngineName();

    protected String openGraphScript;
    protected String traversalScript;
    protected String withSugarScript;

    private GraphComputer graphComputer;

    private GroovyTraversalScript(final String traversalScript) {
        this.traversalScript = traversalScript.concat("\n");
    }

    public static <S, E> GroovyTraversalScript<S, E> of(final String traversalScript) {
        return new GroovyTraversalScript<>(traversalScript);
    }

    @Override
    public GroovyTraversalScript<S, E> over(final Graph graph) {
        final Configuration configuration = graph.configuration();
        final StringBuilder configurationMap = new StringBuilder("g = GraphFactory.open([");
        configuration.getKeys().forEachRemaining(key -> configurationMap.append("'").append(key).append("':'").append(configuration.getProperty(key)).append("',"));
        configurationMap.deleteCharAt(configurationMap.length() - 1).append("])\n");
        this.openGraphScript = configurationMap.toString();
        return this;
    }

    @Override
    public GroovyTraversalScript<S, E> using(final GraphComputer graphComputer) {
        this.graphComputer = graphComputer;
        return this;
    }


    public GroovyTraversalScript<S, E> withSugar() {
        this.withSugarScript = SugarLoader.class.getCanonicalName() + ".load()\n";
        return this;
    }

    @Override
    public Future<ComputerResult> result() {
        return this.graphComputer.program(this.program()).submit();
    }

    @Override
    public Future<Traversal<S, E>> traversal() {
        return CompletableFuture.<Traversal<S, E>>supplyAsync(() -> {
            try {
                final TraversalVertexProgram vertexProgram = this.program();
                final ComputerResult result = this.graphComputer.program(vertexProgram).submit().get();
                final GraphTraversal.Admin<S, S> traversal = new DefaultGraphTraversal<>(result.graph().getClass());
                return traversal.addStep(new ComputerResultStep<>(traversal, result, vertexProgram, true));
            } catch (final Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        });
    }

    @Override
    public TraversalVertexProgram program() {
        return TraversalVertexProgram.build().traversal(ENGINE_NAME, this.makeFullScript()).create();
    }

    private String makeFullScript() {
        final StringBuilder builder = new StringBuilder();
        if (null != this.withSugarScript)
            builder.append(this.withSugarScript);
        if (null != this.openGraphScript)
            builder.append(this.openGraphScript);
        if (null != this.traversalScript)
            builder.append(this.traversalScript);
        return builder.toString();
    }
}
