package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.LocallyTraversable;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexStep<E extends Element> extends FlatMapStep<Vertex, E> implements Reversible, LocallyTraversable<E> {

    private final String[] edgeLabels;
    private Direction direction;
    private final Class<E> returnClass;
    private Traversal<E, E> localTraversal = null;

    public VertexStep(final Traversal traversal, final Class<E> returnClass, final Direction direction, final String... edgeLabels) {
        super(traversal);
        this.direction = direction;
        this.edgeLabels = edgeLabels;
        this.returnClass = returnClass;
        VertexStep.generateFunction(this);
    }

    @Override
    public void reverse() {
        this.direction = this.direction.opposite();
    }

    public Direction getDirection() {
        return this.direction;
    }

    public String[] getEdgeLabels() {
        return this.edgeLabels;
    }

    public Class<E> getReturnClass() {
        return this.returnClass;
    }

    @Override
    public void setLocalTraversal(final Traversal<E, E> localTraversal) {
        this.localTraversal = localTraversal;
    }

    @Override
    public Traversal<E, E> getLocalTraversal() {
        return this.localTraversal;
    }

    @Override
    public VertexStep<E> clone() throws CloneNotSupportedException {
        final VertexStep<E> clone = (VertexStep<E>) super.clone();
        if (null != this.localTraversal) clone.localTraversal = this.localTraversal.clone();
        VertexStep.generateFunction(clone);
        return clone;
    }

    @Override
    public String toString() {
        return this.edgeLabels.length > 0 ?
                TraversalHelper.makeStepString(this, this.direction, Arrays.toString(this.edgeLabels), this.returnClass.getSimpleName().toLowerCase(), this.localTraversal) :
                TraversalHelper.makeStepString(this, this.direction, this.returnClass.getSimpleName().toLowerCase(), this.localTraversal);
    }

    ///////////////////////////////

    private static <E extends Element> void generateFunction(final VertexStep<E> vertexStep) {
        if (Vertex.class.isAssignableFrom(vertexStep.returnClass))
            vertexStep.setFunction(traverser -> (Iterator<E>) traverser.get().iterators().vertexIterator(vertexStep.direction, vertexStep.edgeLabels));
        else
            vertexStep.setFunction(traverser -> {
                final Iterator<E> iterator = (Iterator<E>) traverser.get().iterators().edgeIterator(vertexStep.direction, vertexStep.edgeLabels);
                if (null == vertexStep.localTraversal)
                    return iterator;
                else {
                    vertexStep.localTraversal.reset();
                    TraversalHelper.getStart(vertexStep.localTraversal).addPlainStarts(iterator, traverser.bulk());
                    return (Iterator<E>) vertexStep.localTraversal;
                }
            });
    }

}
