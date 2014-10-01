package com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.CountCapStep;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.configuration.Configuration;
import org.javatuples.Pair;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CountCapMapReduce implements MapReduce<MapReduce.NullObject, Long, MapReduce.NullObject, Long, Long> {

    public static final String COUNT_CAP_STEP_SIDE_EFFECT_KEY = "gremlin.countCapStep.sideEffectKey";

    private String sideEffectKey;

    public CountCapMapReduce() {

    }

    public CountCapMapReduce(final CountCapStep step) {
        this.sideEffectKey = step.getSideEffectKey();
    }

    @Override
    public void storeState(final Configuration configuration) {
        configuration.setProperty(COUNT_CAP_STEP_SIDE_EFFECT_KEY, this.sideEffectKey);
    }

    @Override
    public void loadState(final Configuration configuration) {
        this.sideEffectKey = configuration.getString(COUNT_CAP_STEP_SIDE_EFFECT_KEY);
    }


    @Override
    public boolean doStage(final Stage stage) {
        return true;
    }

    @Override
    public void map(Vertex vertex, MapEmitter<MapReduce.NullObject, Long> emitter) {
        final Property<Map<String, Object>> sideEffectProperty = vertex.property(Traversal.SideEffects.DISTRIBUTED_SIDE_EFFECTS_VERTEX_PROPERTY_KEY);
        if (sideEffectProperty.isPresent()) {
            emitter.emit(NullObject.instance(),
                    sideEffectProperty.value().containsKey(this.sideEffectKey) ?
                            ((AtomicLong) sideEffectProperty.value().get(this.sideEffectKey)).get() : 0l);
        } else {
            emitter.emit(NullObject.instance(), 0l);
        }
    }

    @Override
    public void reduce(final NullObject key, final Iterator<Long> values, final ReduceEmitter<NullObject, Long> emitter) {
        long count = 0l;
        while (values.hasNext()) {
            count = values.next() + count;
        }
        emitter.emit(NullObject.instance(), count);
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
        return this.sideEffectKey;
    }
}