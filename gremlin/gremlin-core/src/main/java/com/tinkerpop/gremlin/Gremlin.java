package com.tinkerpop.gremlin;

import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.query.util.HasContainer;
import com.tinkerpop.blueprints.query.util.VertexQueryBuilder;
import com.tinkerpop.gremlin.pipes.filter.HasPipe;
import com.tinkerpop.gremlin.pipes.map.VertexQueryPipe;

import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Gremlin<S, E> extends Iterator<E> {
    public void addStarts(final Iterator<Holder<S>> starts);

    public <S, E> Gremlin<S, E> addPipe(final Pipe<?, E> pipe);

    public List<Pipe> getPipes();

    public default <E2> Gremlin<S, E2> has(final String key, final Object value) {
        return this.addPipe(new HasPipe<>(this, new HasContainer(key, Compare.EQUAL, value)));
    }

    public default Gremlin<S, Vertex> out(final int branchFactor, final String... labels) {
        return this.addPipe(new VertexQueryPipe<>(this, new VertexQueryBuilder().direction(Direction.OUT).limit(branchFactor).labels(labels), Vertex.class));
    }

    public default Gremlin<S, Vertex> out(final String... labels) {
        return this.out(Integer.MAX_VALUE, labels);
    }
}