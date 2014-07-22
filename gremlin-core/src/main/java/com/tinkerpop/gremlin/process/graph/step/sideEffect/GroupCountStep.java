package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Bulkable;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
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
    public SFunction<S, ?> preGroupFunction;
    public final String variable;
    private long bulkCount = 1l;

    public GroupCountStep(final Traversal traversal, final String variable, final SFunction<S, ?> preGroupFunction) {
        super(traversal);
        this.preGroupFunction = preGroupFunction;
        this.variable = variable;
        this.groupCountMap = this.traversal.memory().getOrCreate(variable, HashMap<Object, Long>::new);
        this.setPredicate(traverser -> {
            MapHelper.incr(this.groupCountMap,
                    null == this.preGroupFunction ? traverser.get() : this.preGroupFunction.apply(traverser.get()),
                    this.bulkCount);
            return true;
        });
    }

    public GroupCountStep(final Traversal traversal, final String variable) {
        this(traversal, variable, null);
    }

    public void setCurrentBulkCount(final long bulkCount) {
        this.bulkCount = bulkCount;
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
