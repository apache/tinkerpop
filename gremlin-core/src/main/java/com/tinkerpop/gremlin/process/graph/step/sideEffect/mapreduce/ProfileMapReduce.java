package com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.ProfileStep;
import com.tinkerpop.gremlin.process.util.TraversalMetrics;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.javatuples.Pair;

import java.util.Iterator;

public final class ProfileMapReduce implements MapReduce<MapReduce.NullObject, TraversalMetrics, MapReduce.NullObject, TraversalMetrics, TraversalMetrics> {

    public ProfileMapReduce() {
    }

    @Override
    public boolean doStage(final Stage stage) {
        return true;
    }

    @Override
    public String getMemoryKey() {
        return ProfileStep.METRICS_KEY;
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<NullObject, TraversalMetrics> emitter) {
        TraversalVertexProgram.getLocalSideEffects(vertex).<TraversalMetrics>ifPresent(ProfileStep.METRICS_KEY, emitter::emit);
    }

    @Override
    public void combine(final NullObject key, final Iterator<TraversalMetrics> values, final ReduceEmitter<NullObject, TraversalMetrics> emitter) {
        reduce(key, values, emitter);
    }

    @Override
    public void reduce(final NullObject key, final Iterator<TraversalMetrics> values, final ReduceEmitter<NullObject, TraversalMetrics> emitter) {
        emitter.emit(TraversalMetrics.merge(values));
    }

    @Override
    public TraversalMetrics generateFinalResult(final Iterator<Pair<NullObject, TraversalMetrics>> keyValues) {
        return TraversalMetrics.merge(new Iterator<TraversalMetrics>() {
            @Override
            public boolean hasNext() {
                return keyValues.hasNext();
            }

            @Override
            public TraversalMetrics next() {
                return keyValues.next().getValue1();
            }
        });
    }

    @Override
    public String toString() {
        return StringFactory.mapReduceString(this, "");
    }
}
