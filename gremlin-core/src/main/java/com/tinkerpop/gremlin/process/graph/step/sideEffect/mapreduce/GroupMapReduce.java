package com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.computer.KeyValue;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroupStep;
import com.tinkerpop.gremlin.process.util.BulkSet;
import com.tinkerpop.gremlin.process.util.TraversalMatrix;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration.Configuration;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GroupMapReduce implements MapReduce<Object, Collection, Object, Object, Map> {

    public static final String GROUP_BY_STEP_SIDE_EFFECT_KEY = "gremlin.groupStep.sideEffectKey";
    public static final String GROUP_BY_STEP_STEP_ID = "gremlin.groupStep.stepId";

    private String sideEffectKey;
    private Traversal.Admin<?, ?> traversal;
    private String groupStepId;
    private Function reduceFunction;
    private Supplier<Map> mapSupplier;

    private GroupMapReduce() {

    }

    public GroupMapReduce(final GroupStep step) {
        this.groupStepId = step.getId();
        this.sideEffectKey = step.getSideEffectKey();
        this.reduceFunction = step.getReduceFunction();
        this.traversal = step.getTraversal().asAdmin();
        this.mapSupplier = this.traversal.getSideEffects().<Map>getRegisteredSupplier(this.sideEffectKey).orElse(HashMap::new);
    }

    @Override
    public void storeState(final Configuration configuration) {
        MapReduce.super.storeState(configuration);
        configuration.setProperty(GROUP_BY_STEP_SIDE_EFFECT_KEY, this.sideEffectKey);
        configuration.setProperty(GROUP_BY_STEP_STEP_ID, this.groupStepId);
    }

    @Override
    public void loadState(final Configuration configuration) {
        this.sideEffectKey = configuration.getString(GROUP_BY_STEP_SIDE_EFFECT_KEY);
        this.groupStepId = configuration.getString(GROUP_BY_STEP_STEP_ID);
        this.traversal = TraversalVertexProgram.getTraversalSupplier(configuration).get();
        this.traversal.applyStrategies(TraversalEngine.COMPUTER); // TODO: this is a scary error prone requirement, but only a problem for GroupStep
        final GroupStep groupStep = new TraversalMatrix<>(this.traversal).getStepById(this.groupStepId);
        this.reduceFunction = groupStep.getReduceFunction();
        this.mapSupplier = this.traversal.getSideEffects().<Map>getRegisteredSupplier(this.sideEffectKey).orElse(HashMap::new);
    }

    @Override
    public boolean doStage(final Stage stage) {
        return !stage.equals(Stage.COMBINE);
    }

    @Override
    public void map(Vertex vertex, MapEmitter<Object, Collection> emitter) {
        this.traversal.getSideEffects().setLocalVertex(vertex);
        this.traversal.getSideEffects().<Map<Object, Collection>>orElse(this.sideEffectKey, Collections.emptyMap()).forEach(emitter::emit);
    }

    @Override
    public void reduce(final Object key, final Iterator<Collection> values, final ReduceEmitter<Object, Object> emitter) {
        final Set set = new BulkSet<>();
        values.forEachRemaining(set::addAll);
        emitter.emit(key, (null == this.reduceFunction) ? set : this.reduceFunction.apply(set));
    }

    @Override
    public Map generateFinalResult(final Iterator<KeyValue<Object, Object>> keyValues) {
        final Map map = this.mapSupplier.get();
        keyValues.forEachRemaining(keyValue -> map.put(keyValue.getKey(), keyValue.getValue()));
        return map;
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

    @Override
    public GroupMapReduce clone() throws CloneNotSupportedException {
        final GroupMapReduce clone = (GroupMapReduce) super.clone();
        clone.traversal = this.traversal.clone().asAdmin();
        return clone;
    }
}