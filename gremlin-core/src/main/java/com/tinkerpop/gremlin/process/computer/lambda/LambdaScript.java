package com.tinkerpop.gremlin.process.computer.lambda;

import com.tinkerpop.gremlin.process.computer.ComputerResult;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.structure.Graph;

import java.util.concurrent.Future;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface LambdaScript {

    public LambdaScript over(final Graph graph);

    public LambdaScript using(final GraphComputer graphComputer);

    public LambdaScript setup(final String setupScript);

    public LambdaScript execute(final String executeScript);

    public LambdaScript terminate(final String terminateScript);

    public LambdaScript map(final String mapScript);

    public LambdaScript combine(final String combineScript);

    public LambdaScript reduce(final String reduceScript);

    public LambdaScript memory(final String memoryScript);

    public LambdaScript memoryKey(final String memoryKey);

    public LambdaScript memoryComputeKeys(final String... memoryComputeKeys);

    public LambdaScript elementComputeKeys(final String... elementComputeKeys);

    public LambdaVertexProgram program();

    public Future<ComputerResult> result();
}
