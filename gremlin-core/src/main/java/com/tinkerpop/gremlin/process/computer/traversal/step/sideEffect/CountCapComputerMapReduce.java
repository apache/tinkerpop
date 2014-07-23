package com.tinkerpop.gremlin.process.computer.traversal.step.sideEffect;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapable;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.configuration.Configuration;
import org.javatuples.Pair;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CountCapComputerMapReduce implements MapReduce<MapReduce.NullObject, Long, MapReduce.NullObject, Long, Long> {

    public static final String COUNT_VARIABLE = "gremlin.countStep.variable";

    private String variable;

    public CountCapComputerMapReduce() {

    }

    public CountCapComputerMapReduce(final CountCapComputerStep step) {
        this.variable = step.getVariable();
    }

    @Override
    public void stageConfiguration(final Configuration configuration) {
        configuration.setProperty(COUNT_VARIABLE, this.variable);
    }

    public void setup(final Configuration configuration) {
        this.variable = configuration.getString(COUNT_VARIABLE);
    }

    @Override
    public String getResultVariable() {
        return SideEffectCapable.CAP_KEY;
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
    public Long getResult(Iterator<Pair<NullObject, Long>> keyValues) {
        return keyValues.next().getValue1();
    }
}