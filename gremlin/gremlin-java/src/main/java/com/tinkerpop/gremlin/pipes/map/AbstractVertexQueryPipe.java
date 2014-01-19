package com.tinkerpop.gremlin.pipes.map;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.query.util.VertexQueryBuilder;
import com.tinkerpop.gremlin.FlatMapPipe;
import com.tinkerpop.gremlin.Pipeline;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class AbstractVertexQueryPipe<E extends Element> extends FlatMapPipe<Vertex, E> {

    public VertexQueryBuilder queryBuilder;

    public AbstractVertexQueryPipe(final Pipeline pipeline) {
        super(pipeline);
    }

    public String toString() {
        return this.getClass().getSimpleName() + "[" + this.queryBuilder.toString() + "]";
    }


}
