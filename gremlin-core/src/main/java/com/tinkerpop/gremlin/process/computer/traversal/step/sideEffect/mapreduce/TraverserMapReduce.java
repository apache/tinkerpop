package com.tinkerpop.gremlin.process.computer.traversal.step.sideEffect.mapreduce;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.KeyValue;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import com.tinkerpop.gremlin.process.computer.util.StaticMapReduce;
import com.tinkerpop.gremlin.process.graph.traversal.step.ComparatorHolder;
import com.tinkerpop.gremlin.process.traversal.step.Reducing;
import com.tinkerpop.gremlin.process.util.TraverserSet;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.commons.configuration.Configuration;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.BiFunction;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraverserMapReduce extends StaticMapReduce<Comparable, Object, Comparable, Object, Iterator<Object>> {

    public static final String TRAVERSERS = Graph.Hidden.hide("traversers");

    private Traversal.Admin<?, ?> traversal;
    private Optional<Comparator<Comparable>> comparator = Optional.empty();
    private Optional<Reducing.Reducer> reducer = Optional.empty();

    private TraverserMapReduce() {
    }

    public TraverserMapReduce(final Step traversalEndStep) {
        this.traversal = traversalEndStep.getTraversal();
        this.comparator = Optional.ofNullable(traversalEndStep instanceof ComparatorHolder ? GraphComputerHelper.chainComparators(((ComparatorHolder) traversalEndStep).getComparators()) : null);
        this.reducer = Optional.ofNullable(traversalEndStep instanceof Reducing ? ((Reducing) traversalEndStep).getReducer() : null);
    }

    @Override
    public void loadState(final Configuration configuration) {
        this.traversal = TraversalVertexProgram.getTraversalSupplier(configuration).get();
        final Step endStep = this.traversal.getEndStep();
        this.comparator = Optional.ofNullable(endStep instanceof ComparatorHolder ? GraphComputerHelper.chainComparators(((ComparatorHolder) endStep).getComparators()) : null);
        this.reducer = Optional.ofNullable(endStep instanceof Reducing ? ((Reducing) endStep).getReducer() : null);
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
        Object mutatingSeed = this.reducer.get().getSeedSupplier().get();
        final BiFunction function = this.reducer.get().getBiFunction();
        final boolean onTraverser = this.reducer.get().onTraverser();
        while (values.hasNext()) {
            mutatingSeed = function.apply(mutatingSeed, onTraverser ? values.next() : ((Traverser) values.next()).get());
        }
        emitter.emit(key, this.traversal.getTraverserGenerator().generate(Reducing.FinalGet.tryFinalGet(mutatingSeed), (Step) this.traversal.getEndStep(), 1l));
    }

    @Override
    public Optional<Comparator<Comparable>> getMapKeySort() {
        return this.comparator;
    }

    @Override
    public Iterator<Object> generateFinalResult(final Iterator<KeyValue<Comparable, Object>> keyValues) {
        if (this.reducer.isPresent() && !keyValues.hasNext())
            return IteratorUtils.of(this.traversal.getTraverserGenerator().generate(this.reducer.get().getSeedSupplier().get(), (Step) this.traversal.getEndStep(), 1l));
        else {
            return new Iterator<Object>() {
                @Override
                public boolean hasNext() {
                    return keyValues.hasNext();
                }

                @Override
                public Object next() {
                    return keyValues.next().getValue();
                }
            };
        }
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
        return StringFactory.mapReduceString(this);
    }
}