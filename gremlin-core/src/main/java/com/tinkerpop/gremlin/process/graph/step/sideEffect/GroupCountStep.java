package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Bulkable;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.util.FunctionRing;
import com.tinkerpop.gremlin.process.util.MapHelper;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.util.function.SFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroupCountStep<S> extends FilterStep<S> implements SideEffectCapable, Reversible, Bulkable {

    public Map<Object, Long> groupCountMap;
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

    public String toString() {
        return this.variable.equals(SideEffectCapable.CAP_KEY) ?
                super.toString() :
                TraversalHelper.makeStepString(this, this.variable);
    }

    @Override
    public <A, B> void rehydrateStep(final Traversal<A, B> traversal) {
        super.rehydrateStep(traversal);
        this.groupCountMap = this.traversal.memory().getOrCreate(this.variable, HashMap<Object, Long>::new);
    }

    @Override
    public void dehydrateStep() {
        super.dehydrateStep();
        this.groupCountMap = null;
    }

}
