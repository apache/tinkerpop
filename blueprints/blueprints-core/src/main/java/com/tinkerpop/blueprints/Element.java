package com.tinkerpop.blueprints;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Element extends Thing, Featureable {

    public Object getId();

    public String getLabel();

    public void remove();

    public interface Features extends com.tinkerpop.blueprints.Features {

        public static IllegalArgumentException bothIsNotSupported() {
            return new IllegalArgumentException("A direction of BOTH is not supported");
        }
    }

}
