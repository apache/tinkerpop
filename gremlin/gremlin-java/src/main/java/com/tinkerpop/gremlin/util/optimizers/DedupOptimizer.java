package com.tinkerpop.gremlin.util.optimizers;

import com.tinkerpop.gremlin.Optimizer;
import com.tinkerpop.gremlin.Pipe;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.oltp.filter.DedupPipe;
import com.tinkerpop.gremlin.oltp.map.IdentityPipe;
import com.tinkerpop.gremlin.oltp.map.OrderPipe;
import com.tinkerpop.gremlin.oltp.map.PropertyPipe;
import com.tinkerpop.gremlin.util.GremlinHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DedupOptimizer implements Optimizer.FinalOptimizer {

    private static final List<Class> BIJECTIVE_PIPES = new ArrayList<Class>(
            Arrays.asList(
                    IdentityPipe.class,
                    PropertyPipe.class,
                    OrderPipe.class
            ));

    public void optimize(final Pipeline pipeline) {
        boolean done = false;
        while (!done) {
            done = true;
            for (int i = 0; i < pipeline.getPipes().size(); i++) {
                final Pipe pipe1 = (Pipe) pipeline.getPipes().get(i);
                if (pipe1 instanceof DedupPipe && !((DedupPipe) pipe1).hasUniqueFunction) {
                    for (int j = i; j >= 0; j--) {
                        final Pipe pipe2 = (Pipe) pipeline.getPipes().get(j);
                        if (BIJECTIVE_PIPES.stream().filter(c -> c.isAssignableFrom(pipe2.getClass())).findFirst().isPresent()) {
                            GremlinHelper.removePipe(pipe1, pipeline);
                            GremlinHelper.insertPipe(pipe1, j, pipeline);
                            done = false;
                            break;
                        }
                    }
                }
                if (!done)
                    break;
            }
        }
    }
}
