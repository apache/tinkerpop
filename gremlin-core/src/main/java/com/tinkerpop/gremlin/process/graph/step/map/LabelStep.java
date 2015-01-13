package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.structure.Element;

import java.util.Collections;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class LabelStep<E extends Element> extends MapStep<E, String> {

    public LabelStep(final Traversal traversal) {
        super(traversal);
        this.setFunction(traverser -> traverser.get().label());
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }
}
