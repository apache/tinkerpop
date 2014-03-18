package com.tinkerpop.gremlin.process.graph.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexStep<E extends Element> extends FlatMapStep<Vertex, E> {

    public String[] labels;
    public Direction direction;
    public int branchFactor;
    //public Class<E> returnClass;

    public VertexStep(final Traversal traversal, final Class<E> returnClass, final Direction direction, final int branchFactor, final String... labels) {
        super(traversal);
        this.direction = direction;
        this.labels = labels;
        this.branchFactor = branchFactor;
        //this.returnClass = returnClass;
        if (Vertex.class.isAssignableFrom(returnClass)) {
            this.setFunction(holder -> {
                if (this.direction.equals(Direction.OUT)) {
                    return (Iterator<E>) holder.get().out(branchFactor, this.labels);
                } else if (this.direction.equals(Direction.IN)) {
                    return (Iterator<E>) holder.get().in(branchFactor, this.labels);
                } else {
                    return (Iterator<E>) holder.get().both(branchFactor, this.labels);
                }
            });
        } else {
            this.setFunction(holder -> {
                if (this.direction.equals(Direction.OUT)) {
                    return (Iterator<E>) holder.get().outE(branchFactor, this.labels);
                } else if (this.direction.equals(Direction.IN)) {
                    return (Iterator<E>) holder.get().inE(branchFactor, this.labels);
                } else {
                    return (Iterator<E>) holder.get().bothE(branchFactor, this.labels);
                }
            });
        }
    }
}
