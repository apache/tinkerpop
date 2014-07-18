package com.tinkerpop.gremlin.giraph.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.AggregateStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphAggregateStep extends GiraphStoreStep {

    public GiraphAggregateStep(final Traversal traversal, final AggregateStep aggregateStep) {
        super(traversal);
        this.preStoreFunction = aggregateStep.preAggregateFunction;
        this.variable = aggregateStep.variable;
        this.setPredicate(traverser -> {
            final Object storeObject = (null == this.preStoreFunction) ?
                    ((Traverser) traverser).get() :
                    this.preStoreFunction.apply(((Traverser) traverser).get());
            for (int i = 0; i < this.bulkCount; i++) {
                this.collection.add(storeObject);
            }
            return true;
        });
        if (TraversalHelper.isLabeled(aggregateStep))
            this.setAs(aggregateStep.getAs());
    }
}
