package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.Bulkable;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.marker.VertexCentric;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce.StoreMapReduce;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.SFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StoreStep<S> extends FilterStep<S> implements SideEffectCapable, Reversible, Bulkable, VertexCentric, MapReducer<MapReduce.NullObject, Object, MapReduce.NullObject, Object, List<Object>> {

    public Collection store;
    public long bulkCount = 1l;
    public SFunction<S, ?> preStoreFunction;

    public StoreStep(final Traversal traversal, final SFunction<S, ?> preStoreFunction) {
        super(traversal);
        this.preStoreFunction = preStoreFunction;
        this.store = this.traversal.memory().getOrCreate(this.getAs(), ArrayList::new);
        this.setPredicate(traverser -> {
            final Object storeObject = null == this.preStoreFunction ? traverser.get() : this.preStoreFunction.apply(traverser.get());
            for (int i = 0; i < this.bulkCount; i++) {
                this.store.add(storeObject);
            }
            return true;
        });
    }

    public StoreStep(final Traversal traversal) {
        this(traversal, null);
    }

    @Override
    public void setAs(final String as) {
        this.traversal.memory().move(this.getAs(), as, ArrayList::new);
        super.setAs(as);
    }

    public void setCurrentBulkCount(final long bulkCount) {
        this.bulkCount = bulkCount;
    }

    public void setCurrentVertex(final Vertex vertex) {
        final String hiddenAs = Graph.Key.hide(this.getAs());
        this.store = vertex.<Collection>property(hiddenAs).orElse(new ArrayList());
        if (!vertex.property(hiddenAs).isPresent())
            vertex.property(hiddenAs, this.store);
    }

    public MapReduce<MapReduce.NullObject, Object, MapReduce.NullObject, Object, List<Object>> getMapReduce() {
        return new StoreMapReduce(this);
    }
}
