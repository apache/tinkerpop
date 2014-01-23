package com.tinkerpop.gremlin.oltp.filter;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.query.util.HasContainer;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.util.GremlinHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IntervalPipe extends FilterPipe<Element> {

    public HasContainer startContainer;
    public HasContainer endContainer;

    public IntervalPipe(final Pipeline pipeline, final HasContainer startContainer, final HasContainer endContainer) {
        super(pipeline);
        this.startContainer = startContainer;
        this.endContainer = endContainer;
        this.setPredicate(holder -> startContainer.test(holder.get()) && endContainer.test(holder.get()));
    }

    public String toString() {
        return GremlinHelper.makePipeString(this, this.startContainer, this.endContainer);
    }
}
