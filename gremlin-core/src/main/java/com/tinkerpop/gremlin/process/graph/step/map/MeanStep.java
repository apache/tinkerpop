package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.Reducing;
import com.tinkerpop.gremlin.process.graph.step.util.ReducingBarrierStep;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MeanStep<S extends Number, E extends Number> extends ReducingBarrierStep<S, E> implements Reducing<E, Traverser<S>> {

    private static final Set<TraverserRequirement> REQUIREMENTS = new HashSet<>(Arrays.asList(TraverserRequirement.OBJECT, TraverserRequirement.BULK));

    public MeanStep(final Traversal traversal) {
        super(traversal);
        this.setSeedSupplier(() -> (E) new MeanNumber());
        this.setBiFunction((seed, start) -> (E) ((MeanNumber) seed).add(start.get(), start.bulk()));
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }

    @Override
    public Reducer<E, Traverser<S>> getReducer() {
        return new Reducer<>(this.getSeedSupplier(), this.getBiFunction(), true);
    }

    ///

    public final class MeanNumber extends Number implements Comparable<Number>, FinalGet<Double> {

        private long count = 0l;
        private double sum = 0.0d;

        public MeanNumber add(final Number amount, final long count) {
            this.count = this.count + count;
            this.sum = this.sum + (amount.doubleValue() * count);
            return this;
        }

        @Override
        public int intValue() {
            return (int) (this.sum / this.count);
        }

        @Override
        public long longValue() {
            return (long) (this.sum / this.count);
        }

        @Override
        public float floatValue() {
            return (float) (this.sum / this.count);
        }

        @Override
        public double doubleValue() {
            return this.sum / this.count;
        }

        @Override
        public String toString() {
            return Double.toString(this.doubleValue());
        }

        @Override
        public int compareTo(final Number number) {
            return Double.valueOf(this.doubleValue()).compareTo(number.doubleValue());
        }

        @Override
        public boolean equals(final Object object) {
            return object instanceof Number && Double.valueOf(this.doubleValue()).equals(((Number) object).doubleValue());
        }

        @Override
        public int hashCode() {
            return Double.valueOf(this.doubleValue()).hashCode();
        }

        @Override
        public Double getFinal() {
            return this.doubleValue();
        }
    }

}