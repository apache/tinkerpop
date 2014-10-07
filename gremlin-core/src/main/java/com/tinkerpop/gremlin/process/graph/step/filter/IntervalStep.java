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
public final class IntervalStep<S extends Element> extends FilterStep<S> implements HasContainerHolder, Reversible {

    private final HasContainer startContainer;
    private final HasContainer endContainer;

    public IntervalStep(final Traversal traversal, final HasContainer startContainer, final HasContainer endContainer) {
        super(traversal);
        this.startContainer = startContainer;
        this.endContainer = endContainer;
        this.setPredicate(traverser -> {
            final Element element = traverser.get();
            return startContainer.test(element) && endContainer.test(element);
        });
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, this.startContainer, this.endContainer);
    }

    public List<HasContainer> getHasContainers() {
        return Arrays.asList(this.startContainer, this.endContainer);
    }
}
