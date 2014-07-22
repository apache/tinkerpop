package com.tinkerpop.gremlin.process.computer.traversal.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.Bulkable;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.VertexCentric;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroupCountStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapable;
import com.tinkerpop.gremlin.process.util.MapHelper;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.SFunction;
import org.javatuples.Pair;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroupCountComputerStep<S> extends FilterStep<S> implements SideEffectCapable, Reversible, Bulkable, VertexCentric, MapReducer {

    public Map<Object, Long> groupCountMap;
    public SFunction<S, ?> preGroupFunction;
    public final String variable;
    private long bulkCount = 1l;

    public GroupCountComputerStep(final Traversal traversal, final GroupCountStep groupCountStep) {
        super(traversal);
        this.preGroupFunction = groupCountStep.preGroupFunction;
        this.variable = groupCountStep.variable;
        this.setPredicate(traverser -> {
            MapHelper.incr(this.groupCountMap,
                    null == this.preGroupFunction ? traverser.get() : this.preGroupFunction.apply(traverser.get()),
                    this.bulkCount);
            return true;
        });
    }

    public void setCurrentBulkCount(final long bulkCount) {
        this.bulkCount = bulkCount;
    }

    public void setCurrentVertex(final Vertex vertex) {
        this.groupCountMap = vertex.<java.util.Map<Object, Long>>property(Graph.Key.hidden(this.variable)).orElse(new HashMap<>());
        if (!vertex.property(Graph.Key.hidden(this.variable)).isPresent())
            vertex.property(Graph.Key.hidden(this.variable), this.groupCountMap);
    }

    public MapReduce getMapReduce() {
        return new MapReduce<Object, Long, Object, Long, Map<Object, Long>>() {

            public String getGlobalVariable() {
                return variable;
            }

            public boolean doReduce() {
                return true;
            }

            public void map(final Vertex vertex, final MapEmitter<Object, Long> emitter) {
                final Property<Map<Object, Long>> mapProperty = vertex.property(Graph.Key.hidden(variable));
                if (mapProperty.isPresent())
                    mapProperty.value().forEach((k, v) -> emitter.emit(k, v));
            }

            public void reduce(final Object key, final Iterator<Long> values, final ReduceEmitter<Object, Long> emitter) {
                long counter = 0;
                while (values.hasNext()) {
                    counter = counter + values.next();
                }
                emitter.emit(key, counter);
            }

            public Map<Object, Long> getResult(final Iterator<Pair<Object, Long>> keyValues) {
                final Map<Object, Long> result = new HashMap<>();
                keyValues.forEachRemaining(pair -> result.put(pair.getValue0(), pair.getValue1()));
                return result;
            }
        };
    }

    public String toString() {
        return this.variable.equals(SideEffectCapable.CAP_KEY) ?
                super.toString() :
                TraversalHelper.makeStepString(this, this.variable);
    }

    public String getVariable() {
        return this.variable;
    }
}
