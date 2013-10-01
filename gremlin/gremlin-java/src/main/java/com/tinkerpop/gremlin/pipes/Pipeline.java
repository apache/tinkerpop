package com.tinkerpop.gremlin.pipes;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Pipeline<S, E> extends Pipe<S, E> {

    public <P extends Pipeline> P addPipe(final Pipe pipe);

    public default <P extends Pipeline> P getPipeline() {
        return (P) this;
    }

    public int getAs(final String key);
}
