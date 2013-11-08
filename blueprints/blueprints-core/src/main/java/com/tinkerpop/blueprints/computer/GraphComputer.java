package com.tinkerpop.blueprints.computer;

import com.tinkerpop.blueprints.Featureable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface GraphComputer extends Featureable {

    public GraphComputer isolation(Isolation isolation);

    public GraphComputer program(VertexProgram program);

    public ComputeResult submit();

    public static GraphComputer.Features getFeatures() {
        return new Features() {
        };
    }

    public interface Features extends com.tinkerpop.blueprints.Features {
        public default boolean supportsGraphQuery() {
            return true;
        }

        public default boolean supportsVertexQuery() {
            return true;
        }
    }

}
