package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Bulkable;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.util.FunctionRing;
import com.tinkerpop.gremlin.process.util.MapHelper;
import com.tinkerpop.gremlin.util.function.SFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroupCountStep<S> extends FilterStep<S> implements SideEffectCapable, Reversible, Bulkable {

    public final Map<Object, Long> groupCountMap;
    public FunctionRing<S, ?> functionRing;
    public final String variable;
    private long bulkCount = 1l;

    public GroupCountStep(final Traversal traversal, final String variable, final SFunction<S, ?>... preGroupFunctions) {
        super(traversal);
        this.functionRing = new FunctionRing<>(preGroupFunctions);
        this.variable = variable;
        this.groupCountMap = this.traversal.memory().getOrCreate(variable, HashMap<Object, Long>::new);
        this.setPredicate(traverser -> {
            MapHelper.incr(this.groupCountMap, this.functionRing.next().apply(traverser.get()), this.bulkCount);
            return true;
        });
    }

    public void setCurrentBulkCount(final long bulkCount) {
        this.bulkCount = bulkCount;
    }

}
