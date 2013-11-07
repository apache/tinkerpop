package com.tinkerpop.blueprints.mailbox;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum Address {

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
}
