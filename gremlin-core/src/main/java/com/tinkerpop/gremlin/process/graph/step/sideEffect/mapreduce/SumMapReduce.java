package com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.KeyValue;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SumStep;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration.Configuration;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SumMapReduce implements MapReduce<MapReduce.NullObject, Double, MapReduce.NullObject, Double, Double> {

    private Traversal traversal;

    private SumMapReduce() {

    }

    public SumMapReduce(final SumStep step) {
        this.traversal = step.getTraversal();
    }

    @Override
    public void loadState(final Configuration configuration) {
        this.traversal = TraversalVertexProgram.getTraversalSupplier(configuration).get();
    }

    @Override
    public boolean doStage(final Stage stage) {
        return true;
    }

    @Override
    public void map(Vertex vertex, MapEmitter<MapReduce.NullObject, Double> emitter) {
        this.traversal.asAdmin().getSideEffects().setLocalVertex(vertex);
        emitter.emit(this.traversal.asAdmin().getSideEffects().orElse(SumStep.SUM_KEY, 0.0d));
    }

    @Override
    public void reduce(final NullObject key, final Iterator<Double> values, final ReduceEmitter<NullObject, Double> emitter) {
        double sum = 0.0d;
        while (values.hasNext()) {
            sum = values.next() + sum;
        }
        emitter.emit(sum);
    }

    @Override
    public void combine(final NullObject key, final Iterator<Double> values, final ReduceEmitter<NullObject, Double> emitter) {
        this.reduce(key, values, emitter);
    }

    @Override
    public Double generateFinalResult(final Iterator<KeyValue<NullObject, Double>> keyValues) {
        return keyValues.next().getValue();
    }

    @Override
    public String getMemoryKey() {
        return SumStep.SUM_KEY;
    }

    @Override
    public int hashCode() {
        return (this.getClass().getCanonicalName() + SumStep.SUM_KEY).hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return GraphComputerHelper.areEqual(this, object);
    }

    @Override
    public String toString() {
        return StringFactory.mapReduceString(this);
    }

    @Override
    public SumMapReduce clone() throws CloneNotSupportedException {
        final SumMapReduce clone = (SumMapReduce) super.clone();
        clone.traversal = this.traversal.clone();
        return clone;
    }
}