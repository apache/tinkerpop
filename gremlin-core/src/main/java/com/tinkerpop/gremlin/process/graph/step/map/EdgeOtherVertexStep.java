package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class EdgeOtherVertexStep extends MapStep<Edge, Vertex> {

    public EdgeOtherVertexStep(final Traversal traversal) {
        super(traversal);
        this.setFunction(traverser -> {
            final List<Object> objects = traverser.path().objects();
            for (int i = objects.size() - 2; i >= 0; i--) {
                if (objects.get(i) instanceof Vertex) {
                    return ElementHelper.areEqual((Vertex) objects.get(i), traverser.get().iterators().vertexIterator(Direction.OUT).next()) ?
                            traverser.get().iterators().vertexIterator(Direction.IN).next() :
                            traverser.get().iterators().vertexIterator(Direction.OUT).next();
                }
            }
            throw new IllegalStateException("The path history of the traverser does not contain a previous vertex: " + traverser.path());
        });
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.PATH);
    }
}
