package com.tinkerpop.blueprints.mailbox;

import com.tinkerpop.blueprints.Vertex;

import java.io.Serializable;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Mailbox<M extends Serializable> {

    public Iterable<M> getMessages(Vertex vertex);

    public void sendMessage(Vertex vertex, List<Object> ids, M message);

    /*public enum Address {

        OUT, IN, BOTH, ID, PARTITION;

        private final Object reference;

        private Address() {
            this.reference = null;
        }

        private Address(final Object reference) {
            this.reference = reference;
        }

        public Object getReference() {
            return this.reference;
        }
    }*/
}
