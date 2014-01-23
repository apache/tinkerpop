package com.tinkerpop.gremlin.util.optimizers;

import com.tinkerpop.gremlin.Optimizer;
import com.tinkerpop.gremlin.Pipe;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.oltp.filter.HasPipe;
import com.tinkerpop.gremlin.oltp.filter.IntervalPipe;
import com.tinkerpop.gremlin.oltp.map.GraphQueryPipe;
import com.tinkerpop.gremlin.oltp.map.IdentityPipe;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphQueryOptimizer implements Optimizer.StepOptimizer {

    public boolean optimize(final Pipeline pipeline, final Pipe pipe) {
        if (!(pipe instanceof HasPipe || pipe instanceof IntervalPipe))
            return true;

        GraphQueryPipe graphQueryPipe = null;
        for (int i = pipeline.getPipes().size() - 1; i >= 0; i--) {
            if (pipeline.getPipes().get(i) instanceof GraphQueryPipe) {
                graphQueryPipe = (GraphQueryPipe) pipeline.getPipes().get(i);
                break;
            } else if (!(pipeline.getPipes().get(i) instanceof IdentityPipe
                    || pipeline.getPipes().get(i) instanceof HasPipe
                    || pipeline.getPipes().get(i) instanceof IntervalPipe))
                break;
        }

        if (null != graphQueryPipe) {
            if (pipe instanceof HasPipe) {
                graphQueryPipe.queryBuilder.has(((HasPipe) pipe).hasContainer.key, ((HasPipe) pipe).hasContainer.predicate, ((HasPipe) pipe).hasContainer.value);
            } else if (pipe instanceof IntervalPipe) {
                graphQueryPipe.queryBuilder.has(((IntervalPipe) pipe).startContainer.key, ((IntervalPipe) pipe).startContainer.predicate, ((IntervalPipe) pipe).startContainer.value);
                graphQueryPipe.queryBuilder.has(((IntervalPipe) pipe).endContainer.key, ((IntervalPipe) pipe).endContainer.predicate, ((IntervalPipe) pipe).endContainer.value);
            } else {
                throw new IllegalStateException("This pipe should not be accessible via this optimizer: " + pipe.getClass());
            }

            graphQueryPipe.generateHolderIterator(false);
            return false;
        }
        return true;
    }
}
