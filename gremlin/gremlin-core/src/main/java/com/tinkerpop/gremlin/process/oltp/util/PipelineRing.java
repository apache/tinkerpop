package com.tinkerpop.gremlin.process.oltp.util;

import com.tinkerpop.gremlin.process.Traversal;

import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PipelineRing<S, E> {

    public Traversal<S, E>[] pipelines;
    private int currentPipeline = -1;

    public PipelineRing(final Traversal<S, E>... pipelines) {
        this.pipelines = pipelines;
    }

    public Traversal<S, E> next() {
        this.currentPipeline = (this.currentPipeline + 1) % this.pipelines.length;
        return this.pipelines[this.currentPipeline];
    }

    public void reset() {
        this.currentPipeline = -1;
    }

    public int size() {
        return this.pipelines.length;
    }

    public Stream<Traversal<S, E>> stream() {
        return Stream.of(this.pipelines);
    }

    public void forEach(final Consumer<Traversal<S, E>> consumer) {
        for (int i = 0; i < this.pipelines.length; i++) {
            consumer.accept(this.pipelines[i]);
        }
    }

}
