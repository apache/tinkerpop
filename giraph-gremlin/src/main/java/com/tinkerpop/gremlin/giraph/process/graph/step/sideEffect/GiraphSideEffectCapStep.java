package com.tinkerpop.gremlin.giraph.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.giraph.process.graph.marker.GiraphSideEffectStep;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Bulkable;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapable;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphSideEffectCapStep<S> extends FilterStep<S> implements SideEffectCapable, Reversible, Bulkable, GiraphSideEffectStep {

    public final String variable;

    public GiraphSideEffectCapStep(final Traversal traversal, final SideEffectCapStep step) {
        super(traversal);
        this.variable = step.variable;
        this.setPredicate(traverser -> true);
    }

    public void setCurrentBulkCount(final long bulkCount) {
        // do nothing -- optimization only
    }

    public Object getSideEffect(final Configuration configuration) {
        return ((GiraphSideEffectStep) this.traversal.getSteps().stream()
                .filter(s -> s instanceof GiraphSideEffectStep)
                .filter(s -> ((GiraphSideEffectStep) s).getVariable().equals(this.variable))
                .findFirst().get()).getSideEffect(configuration);
    }

    public String getVariable() {
        return new String();
    }
}