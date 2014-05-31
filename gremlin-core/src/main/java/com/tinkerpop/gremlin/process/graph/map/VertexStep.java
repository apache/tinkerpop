package com.tinkerpop.gremlin.process.graph.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.Reversible;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexStep<E extends Element> extends FlatMapStep<Vertex, E> implements Reversible {

    public String[] labels;
    public Direction direction;
    public int branchFactor;
    public Class<E> returnClass;

    public VertexStep(final Traversal traversal, final Class<E> returnClass, final Direction direction, final int branchFactor, final String... labels) {
        super(traversal);
        this.direction = direction;
        this.labels = labels;
        this.branchFactor = branchFactor;
        this.returnClass = returnClass;
        if (Vertex.class.isAssignableFrom(this.returnClass)) {
            this.setFunction(traverser -> {
                if (this.direction.equals(Direction.OUT)) {
                    return (Iterator<E>) traverser.get().out(branchFactor, this.labels);
                } else if (this.direction.equals(Direction.IN)) {
                    return (Iterator<E>) traverser.get().in(branchFactor, this.labels);
                } else {
                    return (Iterator<E>) traverser.get().both(branchFactor, this.labels);
                }
            });
        } else {
            this.setFunction(traverser -> {
                if (this.direction.equals(Direction.OUT)) {
                    return (Iterator<E>) traverser.get().outE(branchFactor, this.labels);
                } else if (this.direction.equals(Direction.IN)) {
                    return (Iterator<E>) traverser.get().inE(branchFactor, this.labels);
                } else {
                    return (Iterator<E>) traverser.get().bothE(branchFactor, this.labels);
                }
            });
        }
    }

    public void reverse() {
        this.direction = this.direction.opposite();
    }

}
