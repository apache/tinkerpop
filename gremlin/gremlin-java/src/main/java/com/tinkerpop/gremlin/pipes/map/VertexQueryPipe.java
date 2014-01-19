package com.tinkerpop.gremlin.pipes.map;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.query.util.VertexQueryBuilder;
import com.tinkerpop.gremlin.FlatMapPipe;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.pipes.util.GremlinHelper;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexQueryPipe<E extends Element> extends FlatMapPipe<Vertex, E> {

    public VertexQueryBuilder queryBuilder;
    public Class<E> returnClass;

    public VertexQueryPipe(final Pipeline pipeline, final VertexQueryBuilder queryBuilder, final Class<E> returnClass) {
        super(pipeline);
        this.queryBuilder = queryBuilder;
        this.returnClass = returnClass;
        generateFunction();
    }

    public void generateFunction() {
        if (this.returnClass.equals(Vertex.class))
            this.setFunction(v -> (Iterator<E>) this.queryBuilder.build(v.get()).vertices().iterator());
        else
            this.setFunction(v -> (Iterator<E>) this.queryBuilder.build(v.get()).edges().iterator());
    }

    public String toString() {
        return GremlinHelper.makePipeString(this, this.returnClass.getSimpleName().toLowerCase(), this.queryBuilder);
    }


}
