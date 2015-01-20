package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.HasContainerHolder;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.util.HasContainer;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Element;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class HasStep<S extends Element> extends FilterStep<S> implements HasContainerHolder, Reversible {

    private final HasContainer hasContainer;

    public HasStep(final Traversal traversal, final HasContainer hasContainer) {
        super(traversal);
        this.hasContainer = hasContainer;
        this.setPredicate(traverser -> this.hasContainer.test(traverser.get()));
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.hasContainer);
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return Arrays.asList(this.hasContainer);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }
}
