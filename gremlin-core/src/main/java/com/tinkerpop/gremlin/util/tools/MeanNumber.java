package com.tinkerpop.gremlin.util.tools;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MeanNumber extends Number implements Comparable<Number> {

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
}
