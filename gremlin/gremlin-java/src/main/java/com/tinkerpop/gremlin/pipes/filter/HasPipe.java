package com.tinkerpop.gremlin.pipes.filter;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.query.util.HasContainer;
import com.tinkerpop.gremlin.FilterPipe;
import com.tinkerpop.gremlin.Pipeline;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HasPipe extends FilterPipe<Element> {

    public HasContainer hasContainer;

    public HasPipe(final Pipeline<?, Element> pipeline, final HasContainer hasContainer) {
        super(pipeline);
        this.hasContainer = hasContainer;
        this.setPredicate(e -> hasContainer.test(e.get()));
    }

    public String toString() {
        return this.getClass().getSimpleName() + this.hasContainer;
    }
}
