package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.Bulkable;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.marker.VertexCentric;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce.GroupCountMapReduce;
import com.tinkerpop.gremlin.process.util.MapHelper;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.SFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroupCountStep<S> extends FilterStep<S> implements SideEffectCapable, Reversible, Bulkable, VertexCentric, MapReducer<Object, Long, Object, Long, Map<Object, Long>> {

    public Map<Object, Long> groupCountMap;
    public SFunction<S, ?> preGroupFunction;
    private long bulkCount = 1l;
    private final String memoryKey;
    private final String hiddenMemoryKey;

    public GroupCountStep(final Traversal traversal, final String memoryKey, final SFunction<S, ?> preGroupFunction) {
        super(traversal);
        this.preGroupFunction = preGroupFunction;
        this.memoryKey = null == memoryKey ? Graph.Key.hide(UUID.randomUUID().toString()) : memoryKey;
        this.hiddenMemoryKey = Graph.Key.hide(this.memoryKey);
        this.groupCountMap = this.traversal.memory().getOrCreate(this.memoryKey, HashMap::new);
        this.setPredicate(traverser -> {
            MapHelper.incr(this.groupCountMap,
                    null == this.preGroupFunction ? traverser.get() : this.preGroupFunction.apply(traverser.get()),
                    this.bulkCount);
            return true;
        });
    }

    public String getMemoryKey() {
        return this.memoryKey;
    }

    public void setCurrentBulkCount(final long bulkCount) {
        this.bulkCount = bulkCount;
    }

    public void setCurrentVertex(final Vertex vertex) {
        this.groupCountMap = vertex.<java.util.Map<Object, Long>>property(this.hiddenMemoryKey).orElse(new HashMap<>());
        if (!vertex.property(this.hiddenMemoryKey).isPresent())
            vertex.property(this.hiddenMemoryKey, this.groupCountMap);
    }

    public MapReduce<Object, Long, Object, Long, Map<Object, Long>> getMapReduce() {
        return new GroupCountMapReduce(this);
    }
}
