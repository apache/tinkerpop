package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.Bulkable;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.marker.VertexCentric;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce.StoreMapReduce;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.SFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StoreStep<S> extends SideEffectStep<S> implements SideEffectCapable, Reversible, Bulkable, VertexCentric, MapReducer<MapReduce.NullObject, Object, MapReduce.NullObject, Object, List<Object>> {

    public Collection store;
    public long bulkCount = 1l;
    public SFunction<Traverser<S>, ?> preStoreFunction;
    private final String sideEffectKey;
    private final String hiddenSideEffectKey;

    public StoreStep(final Traversal traversal, final String sideEffectKey, final SFunction<Traverser<S>, ?> preStoreFunction) {
        super(traversal);
        this.preStoreFunction = preStoreFunction;
        this.sideEffectKey = null == sideEffectKey ? this.getLabel() : sideEffectKey;
        this.hiddenSideEffectKey = Graph.Key.hide(this.sideEffectKey);
        TraversalHelper.verifySideEffectKeyIsNotAStepLabel(this.sideEffectKey, this.traversal);
        this.store = this.traversal.sideEffects().getOrCreate(this.sideEffectKey, ArrayList::new);
        this.setConsumer(traverser -> {
            final Object storeObject = null == this.preStoreFunction ? traverser.get() : this.preStoreFunction.apply(traverser);
            for (int i = 0; i < this.bulkCount; i++) {
                this.store.add(storeObject);
            }
        });
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    @Override
    public void setCurrentBulkCount(final long bulkCount) {
        this.bulkCount = bulkCount;
    }

    @Override
    public void setCurrentVertex(final Vertex vertex) {
        this.store = vertex.<Collection>property(this.hiddenSideEffectKey).orElse(new ArrayList());
        if (!vertex.property(this.hiddenSideEffectKey).isPresent())
            vertex.property(this.hiddenSideEffectKey, this.store);
    }

    @Override
    public MapReduce<MapReduce.NullObject, Object, MapReduce.NullObject, Object, List<Object>> getMapReduce() {
        return new StoreMapReduce(this);
    }
}
