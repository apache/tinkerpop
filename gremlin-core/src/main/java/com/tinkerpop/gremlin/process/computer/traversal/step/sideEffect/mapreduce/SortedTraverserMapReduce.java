package com.tinkerpop.gremlin.process.computer.traversal.step.sideEffect.mapreduce;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import com.tinkerpop.gremlin.process.util.TraverserSet;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration.Configuration;
import org.javatuples.Pair;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SortedTraverserMapReduce implements MapReduce<Comparable, Object, Comparable, Object, Iterator<Object>> {

    public static final String TRAVERSER_COMPARE = "gremlin.traverser.compare";
    private Compare compare;

    private SortedTraverserMapReduce(){}

    public SortedTraverserMapReduce(final Compare compare) {
        this.compare = compare;
    }

    @Override
    public void storeState(final Configuration configuration) {
        configuration.setProperty(TRAVERSER_COMPARE, this.compare.name());
    }

    @Override
    public void loadState(final Configuration configuration) {
        this.compare = Compare.valueOf(configuration.getString(TRAVERSER_COMPARE));
    }

    @Override
    public boolean doStage(final Stage stage) {
        return null == this.compare || stage.equals(Stage.MAP);
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<Comparable, Object> emitter) {
        vertex.<TraverserSet>property(TraversalVertexProgram.HALTED_TRAVERSERS).ifPresent(traverserSet -> traverserSet.forEach(emitter::emit));
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
        return TraverserMapReduce.TRAVERSERS;
    }

    @Override
    public int hashCode() {
        return (this.getClass().getCanonicalName() + TraverserMapReduce.TRAVERSERS).hashCode();
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