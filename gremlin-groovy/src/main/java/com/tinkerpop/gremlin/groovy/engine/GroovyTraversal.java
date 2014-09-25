package com.tinkerpop.gremlin.groovy.engine;

import com.tinkerpop.gremlin.groovy.function.GSSupplier;
import com.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.ComputerResult;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.traversal.ScriptTraversal;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.process.computer.traversal.step.map.ComputerResultStep;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Graph;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroovyTraversal<S, E> implements ScriptTraversal<S, E> {

    private String traversalScript;
    private Graph graph;
    private GraphComputer graphComputer;
    private boolean withSugar = false;
    private static final String FULL_SCRIPT =
            "g = GraphFactory.open(['gremlin.graph':'%s'])\n" +
                    "traversal = %s;\n" +
                    "traversal.prepareForGraphComputer()\n" +
                    "traversal\n";

    private GroovyTraversal() {
    }

    public static <S, E> GroovyTraversal<S, E> of(final String traversalScript) {
        return (GroovyTraversal) new GroovyTraversal<>().script(traversalScript);
    }

    @Override
    public GroovyTraversal<S, E> script(final String script) {
        this.traversalScript = script;
        return this;
    }

    @Override
    public GroovyTraversal<S, E> over(final Graph graph) {
        this.graph = graph;
        return this;
    }

    @Override
    public GroovyTraversal<S, E> using(final GraphComputer graphComputer) {
        this.graphComputer = graphComputer;
        return this;
    }

    public GroovyTraversal<S, E> withSugar() {
        this.withSugar = true;
        return this;
    }

    @Override
    public Future<ComputerResult> result() {
        return this.graphComputer.program(this.program()).submit();
    }

    @Override
    public Supplier<Traversal<S, E>> supplier() {
        return new GSSupplier<>(String.format(FULL_SCRIPT, this.graph.getClass().getName(), transformToGlobalScan(this.traversalScript)));
    }

    @Override
    public Future<Traversal<S, E>> traversal() {
        return CompletableFuture.<Traversal<S, E>>supplyAsync(() -> {
            try {
                final ComputerResult result = this.result().get();
                final GraphTraversal<S, S> traversal = result.getGraph().<S>of();
                return traversal.addStep(new ComputerResultStep<>(traversal, result, this.program(), true));
            } catch (final Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        });
    }

    @Override
    public TraversalVertexProgram program() {
        this.traversalScript = String.format(FULL_SCRIPT, this.graph.getClass().getName(), transformToGlobalScan(this.traversalScript));
        if (this.withSugar)
            this.traversalScript = SugarLoader.class.getCanonicalName() + ".load()\n" + this.traversalScript;
        //return TraversalVertexProgram.build().traversal(this.traversalScript,"gremlin-groovy").create();
        return TraversalVertexProgram.build().traversal(new GSSupplier<>(String.format(FULL_SCRIPT, this.graph.getClass().getName(), transformToGlobalScan(this.traversalScript)))).create();
    }

    private static String transformToGlobalScan(final String traversalScript) {
        return traversalScript.replaceAll("\\.v\\((.*)\\)\\.", ".V().has(id, $1).").replaceAll("\\.e\\((.*)\\)\\.", ".E().has(id, $1).");
    }

    @Override
    public String name() {
        return "gremlin-groovy";
    }
}
