package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
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
public final class VertexStep<E extends Element> extends FlatMapStep<Vertex, E> implements Reversible {

    private final String[] edgeLabels;
    private Direction direction;
    private final int branchFactor;
    private final Class<E> returnClass;

    public VertexStep(final Traversal traversal, final Class<E> returnClass, final Direction direction, final int branchFactor, final String... edgeLabels) {
        super(traversal);
        this.direction = direction;
        this.edgeLabels = edgeLabels;
        this.branchFactor = branchFactor;
        this.returnClass = returnClass;
        if (Vertex.class.isAssignableFrom(this.returnClass))
            this.setFunction(traverser -> (Iterator<E>) traverser.get().iterators().vertexIterator(this.direction, this.branchFactor, this.edgeLabels));
        else
            this.setFunction(traverser -> (Iterator<E>) traverser.get().iterators().edgeIterator(this.direction, this.branchFactor, this.edgeLabels));
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

    public int getBranchFactor() {
        return this.branchFactor;
    }

    public Class<E> getReturnClass() {
        return this.returnClass;
    }

    public String toString() {
        return this.edgeLabels.length > 0 ?
                TraversalHelper.makeStepString(this, this.direction, Arrays.toString(this.edgeLabels), this.returnClass.getSimpleName().toLowerCase()) :
                TraversalHelper.makeStepString(this, this.direction, this.returnClass.getSimpleName().toLowerCase());
    }

}
