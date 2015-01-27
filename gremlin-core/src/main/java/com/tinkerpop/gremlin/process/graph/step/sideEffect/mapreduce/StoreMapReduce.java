package com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce;

import com.tinkerpop.gremlin.process.computer.KeyValue;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.process.computer.traversal.VertexTraversalSideEffects;
import com.tinkerpop.gremlin.process.computer.util.StaticMapReduce;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StoreStep;
import com.tinkerpop.gremlin.process.util.BulkSet;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.configuration.Configuration;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class StoreMapReduce extends StaticMapReduce<MapReduce.NullObject, Object, MapReduce.NullObject, Object, Collection> {

    public static final String STORE_STEP_SIDE_EFFECT_KEY = "gremlin.storeStep.sideEffectKey";

    private String sideEffectKey;
    private Supplier<Collection> collectionSupplier;

    private StoreMapReduce() {

    }

    public StoreMapReduce(final StoreStep step) {
        this.sideEffectKey = step.getSideEffectKey();
        this.collectionSupplier = step.getTraversal().asAdmin().getSideEffects().<Collection>getRegisteredSupplier(this.sideEffectKey).orElse(BulkSet::new);
    }

    @Override
    public void storeState(final Configuration configuration) {
        super.storeState(configuration);
        configuration.setProperty(STORE_STEP_SIDE_EFFECT_KEY, this.sideEffectKey);
    }

    @Override
    public void loadState(final Configuration configuration) {
        this.sideEffectKey = configuration.getString(STORE_STEP_SIDE_EFFECT_KEY);
        this.collectionSupplier = TraversalVertexProgram.getTraversalSupplier(configuration).get().getSideEffects().<Collection>getRegisteredSupplier(this.sideEffectKey).orElse(BulkSet::new);
    }

    @Override
    public boolean doStage(final Stage stage) {
        return stage.equals(Stage.MAP);
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<NullObject, Object> emitter) {
        VertexTraversalSideEffects.of(vertex).<Collection<?>>orElse(this.sideEffectKey, Collections.emptyList()).forEach(emitter::emit);
    }

    @Override
    public Collection generateFinalResult(final Iterator<KeyValue<NullObject, Object>> keyValues) {
        final Collection collection = this.collectionSupplier.get();
        keyValues.forEachRemaining(pair -> collection.add(pair.getValue()));
        return collection;
    }

    @Override
    public String getMemoryKey() {
        return this.sideEffectKey;
    }
}
