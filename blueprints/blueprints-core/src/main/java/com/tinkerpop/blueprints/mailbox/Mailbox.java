package com.tinkerpop.blueprints.mailbox;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Mailbox<T extends Serializable> {

    public Iterable<T> getMessages();

    public void sendMessage(Address address, T message);
}
