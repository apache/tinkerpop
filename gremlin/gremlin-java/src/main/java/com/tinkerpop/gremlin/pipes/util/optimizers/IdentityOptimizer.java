package com.tinkerpop.gremlin.pipes.util.optimizers;

import com.tinkerpop.gremlin.Optimizer;
import com.tinkerpop.gremlin.Pipe;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.pipes.map.IdentityPipe;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IdentityOptimizer implements Optimizer.FinalOptimizer, Optimizer {

    public Pipeline optimize(final Pipeline pipeline) {
        final Iterator<Pipe<?, ?>> itty = pipeline.getPipes().iterator();
        while (itty.hasNext()) {
            final Pipe<?, ?> pipe = itty.next();
            if (pipe instanceof IdentityPipe && pipe.getAs().startsWith("_"))
                itty.remove();
        }
        return pipeline;
    }
}
