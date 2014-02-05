package com.tinkerpop.gremlin.process.olap.gremlin;

import com.tinkerpop.gremlin.process.Pipe;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.olap.GraphMemory;
import com.tinkerpop.gremlin.process.oltp.map.MapPipe;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReductionPipe<S> extends MapPipe<S, S> {

    public GraphMemory.ReductionMemory reductionMemory;

    public ReductionPipe(final Traversal pipeline, final GraphMemory.ReductionMemory reductionMemory) {
        super(pipeline);
        this.reductionMemory = reductionMemory;
        this.setFunction(s -> {
            this.reductionMemory.emit(s.get(), 1);
            return (S) Pipe.NO_OBJECT;
        });
    }
}
