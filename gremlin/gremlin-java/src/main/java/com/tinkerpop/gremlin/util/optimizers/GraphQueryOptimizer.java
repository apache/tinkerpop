package com.tinkerpop.gremlin.util.optimizers;

import com.tinkerpop.gremlin.Optimizer;
import com.tinkerpop.gremlin.Pipe;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.oltp.filter.HasPipe;
import com.tinkerpop.gremlin.oltp.filter.IntervalPipe;
import com.tinkerpop.gremlin.oltp.map.GraphQueryPipe;
import com.tinkerpop.gremlin.oltp.map.IdentityPipe;
import com.tinkerpop.gremlin.util.GremlinHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphQueryOptimizer implements Optimizer.StepOptimizer {

    private static final List<Class> PIPES_TO_FOLD = new ArrayList<Class>(
            Arrays.asList(
                    IdentityPipe.class,
                    HasPipe.class,
                    IntervalPipe.class));

    public boolean optimize(final Pipeline pipeline, final Pipe pipe) {
        if (!PIPES_TO_FOLD.stream().filter(c -> c.isAssignableFrom(pipe.getClass())).findFirst().isPresent())
            return true;

        GraphQueryPipe graphQueryPipe = null;
        for (int i = pipeline.getPipes().size() - 1; i >= 0; i--) {
            final Pipe tempPipe = (Pipe) pipeline.getPipes().get(i);
            if (tempPipe instanceof GraphQueryPipe) {
                graphQueryPipe = (GraphQueryPipe) tempPipe;
                break;
            } else if (!PIPES_TO_FOLD.stream().filter(c -> c.isAssignableFrom(tempPipe.getClass())).findFirst().isPresent())
                break;
        }

        if (null != graphQueryPipe) {
            if (pipe instanceof HasPipe) {
                final HasPipe hasPipe = (HasPipe) pipe;
                graphQueryPipe.queryBuilder.has(hasPipe.hasContainer.key, hasPipe.hasContainer.predicate, hasPipe.hasContainer.value);
            } else if (pipe instanceof IntervalPipe) {
                final IntervalPipe intervalPipe = (IntervalPipe) pipe;
                graphQueryPipe.queryBuilder.has(intervalPipe.startContainer.key, intervalPipe.startContainer.predicate, intervalPipe.startContainer.value);
                graphQueryPipe.queryBuilder.has(intervalPipe.endContainer.key, intervalPipe.endContainer.predicate, intervalPipe.endContainer.value);
            }
            graphQueryPipe.generateHolderIterator(false);
            return false;
        }
        return true;
    }
}
