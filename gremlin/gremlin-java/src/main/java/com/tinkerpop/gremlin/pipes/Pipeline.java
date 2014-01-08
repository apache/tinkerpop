package com.tinkerpop.gremlin.pipes;

import java.util.List;
import java.util.function.Consumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Pipeline<S, E> extends Pipe<S, E> {

    public <P extends Pipeline> P addPipe(final Pipe pipe);

    public List<Pipe<?, ?>> getPipes();

    public default <P extends Pipeline<?, ?>> P getPipeline() {
        return (P) this;
    }

    public default void forEach(final Consumer<Pipe<?, ?>> consumer) {
        for (int i = 0; i < this.getPipes().size(); i++) {
            consumer.accept(this.getPipes().get(i));
        }
    }
}
