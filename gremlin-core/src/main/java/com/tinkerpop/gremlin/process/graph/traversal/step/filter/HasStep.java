package com.tinkerpop.gremlin.process.graph.traversal.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.traversal.step.HasContainerHolder;
import com.tinkerpop.gremlin.process.traversal.step.Reversible;
import com.tinkerpop.gremlin.process.graph.util.HasContainer;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Element;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class HasStep<S extends Element> extends FilterStep<S> implements HasContainerHolder, Reversible {

    private final HasContainer hasContainer;

    public HasStep(final Traversal.Admin traversal, final HasContainer hasContainer) {
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
        return Collections.singletonList(this.hasContainer);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }
}
