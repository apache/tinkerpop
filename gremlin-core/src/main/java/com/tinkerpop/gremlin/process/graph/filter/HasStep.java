package com.tinkerpop.gremlin.process.graph.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.util.HasContainer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HasStep extends FilterStep<Element> {

    public HasContainer hasContainer;

    public HasStep(final Traversal traversal, final HasContainer hasContainer) {
        super(traversal);
        this.hasContainer = hasContainer;
        this.setPredicate(holder -> hasContainer.test(holder.get()));
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, this.hasContainer);
    }
}
