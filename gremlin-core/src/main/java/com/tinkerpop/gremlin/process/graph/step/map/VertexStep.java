package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexStep<E extends Element> extends FlatMapStep<Vertex, E> implements Reversible {

    private final String[] edgeLabels;
    private Direction direction;
    private final Class<E> returnClass;

    public VertexStep(final Traversal traversal, final Class<E> returnClass, final Direction direction, final String... edgeLabels) {
        super(traversal);
        this.direction = direction;
        this.edgeLabels = edgeLabels;
        this.returnClass = returnClass;
        if (Vertex.class.isAssignableFrom(this.returnClass))
            this.setFunction(traverser -> (Iterator<E>) traverser.get().iterators().vertexIterator(this.direction, this.edgeLabels));
        else
            this.setFunction(traverser -> (Iterator<E>) traverser.get().iterators().edgeIterator(this.direction, this.edgeLabels));
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
    public String toString() {
        return TraversalHelper.makeStepString(this, this.direction, Arrays.asList(this.edgeLabels), this.returnClass.getSimpleName().toLowerCase());
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }
}
