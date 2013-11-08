package com.tinkerpop.blueprints.computer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface GraphComputer {

    public GraphComputer isolation(Isolation isolation);

    public GraphComputer program(VertexProgram program);

    public ComputeResult submit();

}
