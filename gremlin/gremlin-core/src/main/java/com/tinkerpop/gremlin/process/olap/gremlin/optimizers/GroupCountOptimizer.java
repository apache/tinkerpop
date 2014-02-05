package com.tinkerpop.gremlin.process.olap.gremlin.optimizers;

import com.tinkerpop.gremlin.process.Optimizer;
import com.tinkerpop.gremlin.process.Pipe;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.oltp.sideeffect.GroupCountPipe;

import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroupCountOptimizer implements Optimizer.FinalOptimizer {

    public void optimize(final Traversal pipeline) {
        // if pipeline has a GroupCountPipe, replace it with a Gremlin OLAP GroupCountPipe using ReductionMemory
        for (Pipe pipe : (List<Pipe>) pipeline.getPipes()) {
            if (pipe instanceof GroupCountPipe) {
                //pipe.get
            }
        }
    }
}