package com.tinkerpop.gremlin.pipes.filter;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.query.util.HasContainer;
import com.tinkerpop.gremlin.FilterPipe;
import com.tinkerpop.gremlin.Pipeline;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IntervalPipe extends FilterPipe<Element> {

    public HasContainer startContainer;
    public HasContainer endContainer;

    public IntervalPipe(final Pipeline<?, Element> pipeline, final HasContainer startContainer, final HasContainer endContainer) {
        super(pipeline);
        this.startContainer = startContainer;
        this.endContainer = endContainer;
        this.setPredicate(e -> startContainer.test(e.get()) && endContainer.test(e.get()));
    }

    public String toString() {
        return this.getClass().getSimpleName() + "[" + this.startContainer + "," + this.endContainer + "]";
    }
}
