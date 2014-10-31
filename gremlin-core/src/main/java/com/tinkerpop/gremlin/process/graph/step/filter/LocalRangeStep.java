package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.PathConsumer;
import com.tinkerpop.gremlin.process.graph.marker.Ranging;
import com.tinkerpop.gremlin.process.util.BulkSet;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class LocalRangeStep<S extends Element> extends FilterStep<S> implements PathConsumer, Ranging {

    private final long low;
    private final long high;
    private Direction direction = null;
    private final BulkSet<Element> bulkSet = new BulkSet<>();
    private final Set<Element> doneElements = new HashSet<>();

    public LocalRangeStep(final Traversal traversal, final long low, final long high) {
        super(traversal);
        if (low != -1 && high != -1 && low > high) {
            throw new IllegalArgumentException("Not a legal range: [" + low + ", " + high + "]");
        }
        this.low = low;
        this.high = high;

        this.setPredicate(traverser -> {
            Element previousElement = null;
            final Element element = traverser.get();
            if (element instanceof Edge) {
                if (this.direction.equals(Direction.BOTH)) {
                    final List<Object> objects = traverser.path().objects();
                    for (int i = objects.size() - 2; i >= 0; i--) {
                        if (objects.get(i) instanceof Vertex) {
                            previousElement = (Vertex) objects.get(i);
                            break;
                        }
                    }
                } else {
                    previousElement = ((Edge) element).iterators().vertexIterator(this.direction).next();
                }
            } else if (element instanceof Property) {
                previousElement = ((Property) element).element();
            } else {
                throw new IllegalStateException("Only edges and properties can be subject to local range filtering");
            }

            if (this.doneElements.contains(previousElement))
                return false;

            ////////////////
            final long previousElementCounter = this.bulkSet.get(previousElement);

            if (this.high != -1 && previousElementCounter >= this.high) {
                this.doneElements.add(previousElement);
                this.bulkSet.remove(previousElement);
                return false;
            }

            long avail = traverser.bulk();
            if (previousElementCounter + avail <= this.low) {
                // Will not surpass the low w/ this traverser. Skip and filter the whole thing.
                this.bulkSet.add(previousElement, avail);
                return false;
            }

            // Skip for the low and trim for the high. Both can happen at once.

            long toSkip = 0;
            if (previousElementCounter < this.low) {
                toSkip = this.low - previousElementCounter;
            }

            long toTrim = 0;
            if (this.high != -1 && previousElementCounter + avail >= this.high) {
                toTrim = previousElementCounter + avail - this.high;
            }

            long toEmit = avail - toSkip - toTrim;
            this.bulkSet.add(previousElement, toEmit);
            traverser.asAdmin().setBulk(toEmit);

            return true;
        });
    }

    @Override
    public void reset() {
        super.reset();
        this.bulkSet.clear();
        this.doneElements.clear();
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
