package com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.AggregateStep;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.configuration.Configuration;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AggregateMapReduce implements MapReduce<MapReduce.NullObject, Object, MapReduce.NullObject, Object, List<Object>> {

    public static final String AGGREGATE_STEP_SIDE_EFFECT_KEY = "gremlin.aggregateStep.sideEffectKey";

    private String sideEffectKey;

    public AggregateMapReduce() {

    }

    public AggregateMapReduce(final AggregateStep step) {
        this.sideEffectKey = step.getSideEffectKey();
    }

    @Override
    public void storeState(final Configuration configuration) {
        configuration.setProperty(AGGREGATE_STEP_SIDE_EFFECT_KEY, this.sideEffectKey);
    }

    @Override
    public void loadState(final Configuration configuration) {
        this.sideEffectKey = configuration.getString(AGGREGATE_STEP_SIDE_EFFECT_KEY);
    }

    @Override
    public boolean doStage(final Stage stage) {
        return stage.equals(Stage.MAP);
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<NullObject, Object> emitter) {
        vertex.<Map<String, Object>>property(Traversal.SideEffects.DISTRIBUTED_SIDE_EFFECTS_VERTEX_PROPERTY_KEY).ifPresent(sideEffects -> {
            ((Collection) sideEffects.get(this.sideEffectKey)).forEach(object -> emitter.emit(NullObject.instance(), object));
        });
    }

    @Override
    public List<Object> generateSideEffect(final Iterator<Pair<NullObject, Object>> keyValues) {
        final List<Object> result = new ArrayList<>();
        keyValues.forEachRemaining(pair -> result.add(pair.getValue1()));
        return result;
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }
}
