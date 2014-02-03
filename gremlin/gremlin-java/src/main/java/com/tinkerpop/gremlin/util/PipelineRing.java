package com.tinkerpop.gremlin.util;

import com.tinkerpop.gremlin.Pipeline;

import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PipelineRing<S, E> {

    public Pipeline<S, E>[] pipelines;
    private int currentPipeline = -1;

    public PipelineRing(final Pipeline<S, E>... pipelines) {
        this.pipelines = pipelines;
    }

    public Pipeline<S, E> next() {
        this.currentPipeline = (this.currentPipeline + 1) % this.pipelines.length;
        return this.pipelines[this.currentPipeline];
    }

    public void reset() {
        this.currentPipeline = -1;
    }

    public int size() {
        return this.pipelines.length;
    }

    public Stream<Pipeline<S, E>> stream() {
        return Stream.of(this.pipelines);
    }

    public void forEach(final Consumer<Pipeline<S, E>> consumer) {
        for (int i = 0; i < this.pipelines.length; i++) {
            consumer.accept(this.pipelines[i]);
        }
    }

}
