package com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroupCountStep;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.configuration.Configuration;
import org.javatuples.Pair;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroupCountMapReduce implements MapReduce<Object, Long, Object, Long, Map<Object, Long>> {

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
        vertex.<Map<String, Object>>property(Traversal.SideEffects.DISTRIBUTED_SIDE_EFFECTS_VERTEX_PROPERTY_KEY).ifPresent(sideEffects -> {
            ((Map<Object, Number>) sideEffects.get(this.sideEffectKey)).forEach((k, v) -> {
                emitter.emit(k, v.longValue());
            });
        });
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
}
