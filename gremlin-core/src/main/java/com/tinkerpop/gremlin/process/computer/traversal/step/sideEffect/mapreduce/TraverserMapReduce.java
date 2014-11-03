package com.tinkerpop.gremlin.process.computer.traversal.step.sideEffect.mapreduce;

import com.tinkerpop.gremlin.process.SimpleTraverser;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import com.tinkerpop.gremlin.process.graph.marker.Comparing;
import com.tinkerpop.gremlin.process.graph.marker.Reducing;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.process.util.TraverserSet;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration.Configuration;
import org.javatuples.Pair;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraverserMapReduce implements MapReduce<Comparable, Object, Comparable, Object, Iterator<Object>> {

    public static final String TRAVERSERS = Graph.System.system("traversers");

    private Optional<Comparator<Comparable>> comparator = Optional.empty();
    private Optional<Pair<Supplier, BiFunction>> reducer = Optional.empty();

    private TraverserMapReduce() {
    }

    public TraverserMapReduce(final Step traversalEndStep) {
        this.comparator = Optional.ofNullable(traversalEndStep instanceof Comparing ? GraphComputerHelper.chainComparators(((Comparing) traversalEndStep).getComparators()) : null);
        this.reducer = Optional.ofNullable(traversalEndStep instanceof Reducing ? ((Reducing) traversalEndStep).getReducer() : null);
    }

    @Override
    public void storeState(final Configuration configuration) {

    }

    @Override
    public void loadState(final Configuration configuration) {
        final Step step = TraversalHelper.getEnd(TraversalVertexProgram.getTraversalSupplier(configuration).get());
        this.comparator = Optional.ofNullable(step instanceof Comparing ? GraphComputerHelper.chainComparators(((Comparing) step).getComparators()) : null);
        this.reducer = Optional.ofNullable(step instanceof Reducing ? ((Reducing) step).getReducer() : null);
    }

    @Override
    public boolean doStage(final Stage stage) {
        return stage.equals(Stage.MAP) || (stage.equals(Stage.REDUCE) && this.reducer.isPresent());
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<Comparable, Object> emitter) {
        if (this.comparator.isPresent())
            vertex.<TraverserSet<?>>property(TraversalVertexProgram.HALTED_TRAVERSERS).ifPresent(traverserSet -> traverserSet.forEach(traverser -> emitter.emit(traverser, traverser)));
        else
            vertex.<TraverserSet<?>>property(TraversalVertexProgram.HALTED_TRAVERSERS).ifPresent(traverserSet -> traverserSet.forEach(emitter::emit));
    }

    @Override
    public void reduce(final Comparable key, final Iterator<Object> values, final ReduceEmitter<Comparable, Object> emitter) {
        Object mutatingSeed = this.reducer.get().getValue0().get();
        final BiFunction function = this.reducer.get().getValue1();
        while (values.hasNext()) {
            mutatingSeed = function.apply(mutatingSeed, values.next());
        }
        emitter.emit(key, new SimpleTraverser(mutatingSeed, null));
    }

    @Override
    public Optional<Comparator<Comparable>> getMapKeySort() {
        return this.comparator;
    }

    @Override
    public Iterator<Object> generateFinalResult(final Iterator<Pair<Comparable, Object>> keyValues) {
        return new Iterator<Object>() {
            @Override
            public boolean hasNext() {
                return keyValues.hasNext();
            }

            @Override
            public Object next() {
                return keyValues.next().getValue1();
            }
        };
    }

    @Override
    public String getMemoryKey() {
        return TRAVERSERS;
    }

    @Override
    public int hashCode() {
        return (this.getClass().getCanonicalName() + TRAVERSERS).hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return GraphComputerHelper.areEqual(this, object);
    }

    @Override
    public String toString() {
        return StringFactory.mapReduceString(this, "");
    }
}