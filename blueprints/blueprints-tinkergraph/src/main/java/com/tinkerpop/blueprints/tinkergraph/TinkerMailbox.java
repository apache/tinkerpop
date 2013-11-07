package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.mailbox.Mailbox;
import com.tinkerpop.blueprints.util.StreamFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerMailbox<M extends Serializable> implements Mailbox<M> {

    private static final String MAILBOX = Property.Key.hidden("mailbox");

    public Iterable<M> getMessages(Vertex vertex) {
        return StreamFactory.stream(vertex.query().direction(Direction.BOTH).vertices())
                .map(v -> (List) v.getValue(MAILBOX))
                .filter(m -> ((List) m.get(0)).contains(vertex.getId()))
                .map(m -> (M) m.get(1))
                .collect(Collectors.<M>toList());
    }

    public void sendMessage(Vertex vertex, List<Object> ids, M message) {
        vertex.setProperty(MAILBOX, Arrays.asList(ids, message));
    }
}
