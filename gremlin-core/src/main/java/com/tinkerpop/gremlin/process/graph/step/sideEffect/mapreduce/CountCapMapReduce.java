package com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.CountCapStep;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CountCapMapReduce implements MapReduce<MapReduce.NullObject, Long, MapReduce.NullObject, Long, Long> {

    public CountCapMapReduce() {

    }

    public CountCapMapReduce(final CountCapStep step) {
    }

    @Override
    public boolean doStage(final Stage stage) {
        return true;
    }

    @Override
    public void map(Vertex vertex, MapEmitter<MapReduce.NullObject, Long> emitter) {
        final AtomicLong count = vertex.<AtomicLong>property(SideEffectCapable.CAP_KEY).orElse(new AtomicLong(0));
        emitter.emit(NullObject.get(), count.get());
    }

    @Override
    public void reduce(final NullObject key, final Iterator<Long> values, final ReduceEmitter<NullObject, Long> emitter) {
        long count = 0l;
        while (values.hasNext()) {
            count = values.next() + count;
        }
        emitter.emit(NullObject.get(), count);
    }

    @Override
    public void combine(final NullObject key, final Iterator<Long> values, final ReduceEmitter<NullObject, Long> emitter) {
        this.reduce(key, values, emitter);
    }

    @Override
    public Long generateSideEffect(Iterator<Pair<NullObject, Long>> keyValues) {
        return keyValues.next().getValue1();
    }

    @Override
    public String getSideEffectKey() {
        return SideEffectCapable.CAP_KEY;
    }
}