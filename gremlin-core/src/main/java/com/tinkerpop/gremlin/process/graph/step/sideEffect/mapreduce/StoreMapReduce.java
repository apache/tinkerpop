package com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StoreStep;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration.Configuration;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class StoreMapReduce implements MapReduce<MapReduce.NullObject, Object, MapReduce.NullObject, Object, List<Object>> {

    public static final String STORE_STEP_SIDE_EFFECT_KEY = "gremlin.storeStep.sideEffectKey";

    private String sideEffectKey;

    public StoreMapReduce() {

    }

    public StoreMapReduce(final StoreStep step) {
        this.sideEffectKey = step.getSideEffectKey();
    }

    @Override
    public void storeState(final Configuration configuration) {
        configuration.setProperty(STORE_STEP_SIDE_EFFECT_KEY, this.sideEffectKey);
    }

    @Override
    public void loadState(final Configuration configuration) {
        this.sideEffectKey = configuration.getString(STORE_STEP_SIDE_EFFECT_KEY);
    }

    @Override
    public boolean doStage(final Stage stage) {
        return stage.equals(Stage.MAP);
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<NullObject, Object> emitter) {
        TraversalVertexProgram.getLocalSideEffects(vertex).<Collection<?>>orElse(this.sideEffectKey, Collections.emptyList()).forEach(object -> emitter.emit(NullObject.instance(), object));
    }

    @Override
    public List<Object> generateFinalResult(final Iterator<Pair<NullObject, Object>> keyValues) {
        final List<Object> result = new ArrayList<>();
        keyValues.forEachRemaining(pair -> result.add(pair.getValue1()));
        return result;
    }

    @Override
    public String getMemoryKey() {
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

    @Override
    public String toString() {
        return StringFactory.mapReduceString(this, this.sideEffectKey);
    }
}
