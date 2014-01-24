package com.tinkerpop.gremlin.olap;

import com.tinkerpop.blueprints.computer.GraphMemory;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.oltp.map.MapPipe;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReductionPipe<S> extends MapPipe<S, S> {

    public GraphMemory.ReductionMemory reductionMemory;

    public ReductionPipe(final Pipeline pipeline, final GraphMemory.ReductionMemory reductionMemory) {
        super(pipeline);
        this.reductionMemory = reductionMemory;
        this.setFunction(s -> {
            this.reductionMemory.emit(s.get(), 1);
            return (S) NO_OBJECT;
        });
    }
}
