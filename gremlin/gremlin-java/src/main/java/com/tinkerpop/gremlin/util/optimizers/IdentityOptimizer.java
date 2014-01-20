package com.tinkerpop.gremlin.util.optimizers;

import com.tinkerpop.gremlin.Optimizer;
import com.tinkerpop.gremlin.Pipe;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.oltp.map.IdentityPipe;
import com.tinkerpop.gremlin.util.GremlinHelper;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IdentityOptimizer implements Optimizer.FinalOptimizer {

    public Pipeline optimize(final Pipeline pipeline) {
        final Iterator<Pipe<?, ?>> itty = pipeline.getPipes().iterator();
        while (itty.hasNext()) {
            final Pipe<?, ?> pipe = itty.next();
            if (pipe instanceof IdentityPipe && !GremlinHelper.isLabeled(pipe))
                itty.remove();
        }
        return pipeline;
    }
}
