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
public class VertexStep<E extends Element> extends FlatMapStep<Vertex, E> implements Reversible {

    public String[] edgeLabels;
    public Direction direction;
    public int branchFactor;
    public Class<E> returnClass;

    public VertexStep(final Traversal traversal, final Class<E> returnClass, final Direction direction, final int branchFactor, final String... edgeLabels) {
        super(traversal);
        this.direction = direction;
        this.edgeLabels = edgeLabels;
        this.branchFactor = branchFactor;
        this.returnClass = returnClass;
        if (Vertex.class.isAssignableFrom(this.returnClass))
            this.setFunction(traverser -> (Iterator<E>) traverser.get().vertices(this.direction, this.branchFactor, this.edgeLabels));
        else
            this.setFunction(traverser -> (Iterator<E>) traverser.get().edges(this.direction, this.branchFactor, this.edgeLabels));
    }

    public void reverse() {
        this.direction = this.direction.opposite();
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, this.direction, Arrays.asList(this.edgeLabels), this.returnClass.getSimpleName().toLowerCase());
    }

}
