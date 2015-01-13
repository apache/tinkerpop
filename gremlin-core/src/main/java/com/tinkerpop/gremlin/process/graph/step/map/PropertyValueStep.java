package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.structure.Property;

import java.util.Collections;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class PropertyValueStep<E> extends MapStep<Property<E>, E> {

    public PropertyValueStep(final Traversal traversal) {
        super(traversal);
        this.setFunction(traverser -> traverser.get().value());
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }
}
