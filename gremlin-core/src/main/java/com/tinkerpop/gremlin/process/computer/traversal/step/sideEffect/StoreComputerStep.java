package com.tinkerpop.gremlin.process.computer.traversal.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.Bulkable;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.VertexCentric;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StoreStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.SFunction;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StoreComputerStep<S> extends FilterStep<S> implements SideEffectCapable, Reversible, Bulkable, VertexCentric, MapReducer {

    public SFunction<S, ?> preStoreFunction;
    public String variable;
    protected Collection collection;
    protected long bulkCount = 1l;

    public StoreComputerStep(final Traversal traversal, final SFunction<S, ?> preStoreFunction, final String variable) {
        super(traversal);
        this.preStoreFunction = preStoreFunction;
        this.variable = variable;
        this.setPredicate(traverser -> {
            final Object storeObject = null == this.preStoreFunction ? traverser.get() : this.preStoreFunction.apply(traverser.get());
            for (int i = 0; i < this.bulkCount; i++) {
                this.collection.add(storeObject);
            }
            return true;
        });
    }

    public StoreComputerStep(final Traversal traversal, final StoreStep storeStep) {
        this(traversal, storeStep.preStoreFunction, storeStep.variable);
        if (TraversalHelper.isLabeled(storeStep))
            this.setAs(storeStep.getAs());
    }

    public void setCurrentBulkCount(final long bulkCount) {
        this.bulkCount = bulkCount;
    }

    public void setCurrentVertex(final Vertex vertex) {
        this.collection = vertex.<Collection>property(Graph.Key.hidden(this.variable)).orElse(new ArrayList());
        if (!vertex.property(Graph.Key.hidden(this.variable)).isPresent())
            vertex.property(Graph.Key.hidden(this.variable), this.collection);
    }

    public MapReduce getMapReduce() {
        return new StoreComputerMapReduce(this);
    }


    public String getVariable() {
        return this.variable;
    }
}
