package com.tinkerpop.gremlin.process.steps.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.AnnotatedValue;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.util.HasContainer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HasStep<S> extends FilterStep<S> {

    public HasContainer hasContainer;

    public HasStep(final Traversal traversal, final HasContainer hasContainer) {
        super(traversal);
        this.hasContainer = hasContainer;
        this.setPredicate(holder -> {
            final S s = holder.get();
            if (s instanceof Element)
                return hasContainer.test((Element) s);
            else if (s instanceof AnnotatedValue)
                return hasContainer.test((AnnotatedValue) s);
            else
                throw new IllegalArgumentException("The provided class can not be check with has(): " + s.getClass());

        });
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, this.hasContainer);
    }
}
