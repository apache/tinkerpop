package com.tinkerpop.gremlin.process.graph.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.Reversible;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.util.HasContainer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IntervalStep extends FilterStep<Element> implements Reversible {

    public HasContainer startContainer;
    public HasContainer endContainer;

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
}
