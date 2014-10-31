package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.PathConsumer;
import com.tinkerpop.gremlin.process.graph.marker.Ranging;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class LocalRangeStep<S extends Element> extends FilterStep<S> implements PathConsumer, Ranging {

    private final long low;
    private final long high;
    private final AtomicLong counter = new AtomicLong(0l);
    private Direction direction = null;
    private Element previousElement = null;

    public LocalRangeStep(final Traversal traversal, final long low, final long high) {
        super(traversal);
        if (low != -1 && high != -1 && low > high) {
            throw new IllegalArgumentException("Not a legal range: [" + low + ", " + high + "]");
        }
        this.low = low;
        this.high = high;

        this.setPredicate(traverser -> {
            Element currentPreviousElement = null;
            final Element currentElement = traverser.get();
            if (currentElement instanceof Edge) {
                if (this.direction.equals(Direction.BOTH)) {
                    final List<Object> objects = traverser.path().objects();
                    for (int i = objects.size() - 2; i >= 0; i--) {
                        if (objects.get(i) instanceof Vertex) {
                            currentPreviousElement = (Vertex) objects.get(i);
                            break;
                        }
                    }
                } else {
                    currentPreviousElement = ((Edge) currentElement).iterators().vertexIterator(this.direction).next();
                }
            } else if (currentElement instanceof Property) {
                currentPreviousElement = ((Property) currentElement).element();
            } else {
                throw new IllegalStateException("Only edges and properties can be subject to local range filtering");
            }

            if (null == this.previousElement || !ElementHelper.areEqual(currentPreviousElement, this.previousElement)) {
                this.previousElement = currentPreviousElement;
                this.counter.set(0l);
            }

            ////////////////

            if (this.high != -1 && this.counter.get() >= this.high) {
                return false;
            }

            long avail = traverser.bulk();
            if (this.counter.get() + avail <= this.low) {
                // Will not surpass the low w/ this traverser. Skip and filter the whole thing.
                this.counter.getAndAdd(avail);
                return false;
            }

            // Skip for the low and trim for the high. Both can happen at once.

            long toSkip = 0;
            if (this.counter.get() < this.low) {
                toSkip = this.low - this.counter.get();
            }

            long toTrim = 0;
            if (this.high != -1 && this.counter.get() + avail >= this.high) {
                toTrim = this.counter.get() + avail - this.high;
            }

            long toEmit = avail - toSkip - toTrim;
            this.counter.getAndAdd(toSkip + toEmit);
            traverser.asAdmin().setBulk(toEmit);

            return true;
        });
    }

    @Override
    public void reset() {
        super.reset();
        this.counter.set(0l);
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.low, this.high);
    }

    public long getLowRange() {
        return this.low;
    }

    public long getHighRange() {
        return this.high;
    }

    public void setDirection(final Direction direction) {
        this.direction = direction;
    }

    @Override
    public boolean requiresPaths() {
        return Direction.BOTH.equals(this.direction);
    }
}
