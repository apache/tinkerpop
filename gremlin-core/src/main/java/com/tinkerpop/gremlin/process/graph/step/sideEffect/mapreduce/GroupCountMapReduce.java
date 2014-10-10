package com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroupCountStep;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.configuration.Configuration;
import org.javatuples.Pair;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GroupCountMapReduce implements MapReduce<Object, Long, Object, Long, Map<Object, Long>> {

    public static final String GROUP_COUNT_STEP_SIDE_EFFECT_KEY = "gremlin.groupCountStep.sideEffectKey";

    private String sideEffectKey;

    public GroupCountMapReduce() {

    }

    public GroupCountMapReduce(final GroupCountStep step) {
        this.sideEffectKey = step.getSideEffectKey();
    }

    @Override
    public void storeState(final Configuration configuration) {
        configuration.setProperty(GROUP_COUNT_STEP_SIDE_EFFECT_KEY, this.sideEffectKey);
    }

    @Override
    public void loadState(final Configuration configuration) {
        this.sideEffectKey = configuration.getString(GROUP_COUNT_STEP_SIDE_EFFECT_KEY);
    }

    @Override
    public boolean doStage(final Stage stage) {
        return true;
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<Object, Long> emitter) {
        MapReduce.getLocalSideEffects(vertex).<Map<Object, Number>>orElse(this.sideEffectKey, Collections.emptyMap()).forEach((k, v) -> emitter.emit(k, v.longValue()));
    }

    @Override
    public void reduce(final Object key, final Iterator<Long> values, final ReduceEmitter<Object, Long> emitter) {
        long counter = 0;
        while (values.hasNext()) {
            counter = counter + values.next();
        }
        emitter.emit(key, counter);
    }

    @Override
    public void combine(final Object key, final Iterator<Long> values, final ReduceEmitter<Object, Long> emitter) {
        reduce(key, values, emitter);
    }

    @Override
    public Map<Object, Long> generateSideEffect(final Iterator<Pair<Object, Long>> keyValues) {
        final Map<Object, Long> result = new HashMap<>();
        keyValues.forEachRemaining(pair -> result.put(pair.getValue0(), pair.getValue1()));
        return result;
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    @Override
    public int hashCode() {
        return (this.getClass().getCanonicalName() + this.sideEffectKey).hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return GraphComputerHelper.areEqual(this, object);
    }
}
