package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.HasContainerHolder;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.util.HasContainer;

import java.util.Arrays;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class HasStep<S extends Element> extends FilterStep<S> implements HasContainerHolder, Reversible {

    private final HasContainer hasContainer;

    public HasStep(final Traversal traversal, final HasContainer hasContainer) {
        super(traversal);
        this.hasContainer = hasContainer;
        this.setPredicate(traverser -> hasContainer.test(traverser.get()));
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, this.hasContainer);
    }

    public List<HasContainer> getHasContainers() {
        return Arrays.asList(this.hasContainer);
    }
}
