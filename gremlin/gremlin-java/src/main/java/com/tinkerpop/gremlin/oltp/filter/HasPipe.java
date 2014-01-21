package com.tinkerpop.gremlin.oltp.filter;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.query.util.HasContainer;
import com.tinkerpop.gremlin.FilterPipe;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.util.GremlinHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HasPipe extends FilterPipe<Element> {

    public HasContainer hasContainer;

    public HasPipe(final Pipeline pipeline, final HasContainer hasContainer) {
        super(pipeline);
        this.hasContainer = hasContainer;
        this.setPredicate(holder -> hasContainer.test(holder.get()));
    }

    public String toString() {
        return GremlinHelper.makePipeString(this, this.hasContainer);
    }
}
