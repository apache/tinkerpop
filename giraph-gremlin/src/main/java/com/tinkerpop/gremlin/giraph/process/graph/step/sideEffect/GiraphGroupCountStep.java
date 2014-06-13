package com.tinkerpop.gremlin.giraph.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapable;
import com.tinkerpop.gremlin.process.util.FunctionRing;
import com.tinkerpop.gremlin.process.util.MapHelper;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.UnBulkable;
import com.tinkerpop.gremlin.process.graph.marker.VertexCentric;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.SFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGroupCountStep<S> extends FilterStep<S> implements SideEffectCapable, Reversible, UnBulkable, VertexCentric {

    private static final String GIRAPH_GROUP_COUNT = Property.hidden("giraphGroupCount");

    public Map<Object, Long> groupCountMap;
    public FunctionRing<S, ?> functionRing;
    public Vertex vertex;

    public GiraphGroupCountStep(final Traversal traversal, final SFunction<S, ?>... preGroupFunctions) {
        super(traversal);
        this.functionRing = new FunctionRing<>(preGroupFunctions);
        this.setPredicate(traverser -> {
            MapHelper.incr(this.groupCountMap, this.functionRing.next().apply(traverser.get()), 1l);
            return true;
        });
    }

    public void setCurrentVertex(final Vertex vertex) {
        this.groupCountMap = vertex.<Map<Object, Long>>property(GIRAPH_GROUP_COUNT).orElse(new HashMap<>());
        vertex.property(GIRAPH_GROUP_COUNT, this.groupCountMap);
    }
}
