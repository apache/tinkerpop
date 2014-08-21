package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.Bulkable;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.marker.VertexCentric;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce.GroupCountMapReduce;
import com.tinkerpop.gremlin.process.util.MapHelper;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.SFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroupCountStep<S> extends SideEffectStep<S> implements SideEffectCapable, Reversible, Bulkable, VertexCentric, MapReducer<Object, Long, Object, Long, Map<Object, Long>> {

    public Map<Object, Long> groupCountMap;
    public SFunction<S, ?> preGroupFunction;
    private long bulkCount = 1l;
    private final String sideEffectKey;
    private final String hiddenSideEffectKey;

    public GroupCountStep(final Traversal traversal, final String sideEffectKey, final SFunction<S, ?> preGroupFunction) {
        super(traversal);
        this.preGroupFunction = preGroupFunction;
        this.sideEffectKey = null == sideEffectKey ? this.getAs() : sideEffectKey;
        this.hiddenSideEffectKey = Graph.Key.hide(this.sideEffectKey);
        this.groupCountMap = this.traversal.sideEffects().getOrCreate(this.sideEffectKey, HashMap::new);
        this.setConsumer(traverser -> {
            MapHelper.incr(this.groupCountMap,
                    null == this.preGroupFunction ? traverser.get() : this.preGroupFunction.apply(traverser.get()),
                    this.bulkCount);
        });
    }

    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    public void setCurrentBulkCount(final long bulkCount) {
        this.bulkCount = bulkCount;
    }

    public void setCurrentVertex(final Vertex vertex) {
        this.groupCountMap = vertex.<java.util.Map<Object, Long>>property(this.hiddenSideEffectKey).orElse(new HashMap<>());
        if (!vertex.property(this.hiddenSideEffectKey).isPresent())
            vertex.property(this.hiddenSideEffectKey, this.groupCountMap);
    }

    public MapReduce<Object, Long, Object, Long, Map<Object, Long>> getMapReduce() {
        return new GroupCountMapReduce(this);
    }
}
