package com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.CountStep;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.javatuples.Pair;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class CountMapReduce implements MapReduce<MapReduce.NullObject, Long, MapReduce.NullObject, Long, Long> {

    public CountMapReduce() {

    }

    @Override
    public boolean doStage(final Stage stage) {
        return true;
    }

    @Override
    public void map(Vertex vertex, MapEmitter<MapReduce.NullObject, Long> emitter) {
        emitter.emit(TraversalVertexProgram.getLocalSideEffects(vertex).orElse(CountStep.COUNT_KEY, 0l));
    }

    @Override
    public void reduce(final NullObject key, final Iterator<Long> values, final ReduceEmitter<NullObject, Long> emitter) {
        long count = 0l;
        while (values.hasNext()) {
            count = values.next() + count;
        }
        emitter.emit(count);
    }

    @Override
    public void combine(final NullObject key, final Iterator<Long> values, final ReduceEmitter<NullObject, Long> emitter) {
        this.reduce(key, values, emitter);
    }

    @Override
    public Long generateFinalResult(Iterator<Pair<NullObject, Long>> keyValues) {
        long count = 0l;
        while (keyValues.hasNext()) {
            count = count + keyValues.next().getValue1();
        }
        return count;
    }

    @Override
    public String getMemoryKey() {
        return CountStep.COUNT_KEY;
    }

    @Override
    public int hashCode() {
        return (this.getClass().getCanonicalName() + CountStep.COUNT_KEY).hashCode();
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