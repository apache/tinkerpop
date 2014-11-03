package com.tinkerpop.gremlin.process.computer.util;

import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.Memory;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.structure.Graph;

import java.lang.reflect.Method;
import java.util.Comparator;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphComputerHelper {

    public static void validateProgramOnComputer(final GraphComputer computer, final VertexProgram vertexProgram) {
        if (vertexProgram.getMemoryComputeKeys().contains(null))
            throw Memory.Exceptions.memoryKeyCanNotBeNull();
        if (vertexProgram.getMemoryComputeKeys().contains(""))
            throw Memory.Exceptions.memoryKeyCanNotBeEmpty();

        final GraphComputer.Features graphComputerFeatures = computer.features();
        final VertexProgram.Features vertexProgramFeatures = vertexProgram.getFeatures();

        for (final Method method : VertexProgram.Features.class.getMethods()) {
            if (method.getName().startsWith("requires")) {
                final boolean supports;
                final boolean requires;
                try {
                    supports = (boolean) GraphComputer.Features.class.getMethod(method.getName().replace("requires", "supports")).invoke(graphComputerFeatures);
                    requires = (boolean) method.invoke(vertexProgramFeatures);
                } catch (final Exception e) {
                    throw new IllegalStateException("A reflection exception has occurred: " + e.getMessage(), e);
                }
                if (requires && !supports)
                    throw new IllegalStateException("The vertex program can not be executed on the graph computer: " + method.getName());
            }
        }
    }

    public static void validateComputeArguments(Class... graphComputerClass) {
        if (graphComputerClass.length > 1)
            throw Graph.Exceptions.onlyOneOrNoGraphComputerClass();
    }

    public static boolean areEqual(final MapReduce a, final Object b) {
        if (null == a)
            throw Graph.Exceptions.argumentCanNotBeNull("a");
        if (null == b)
            throw Graph.Exceptions.argumentCanNotBeNull("b");

        if (!(b instanceof MapReduce)) return false;
        return a.getClass().equals(b.getClass()) && a.getMemoryKey().equals(((MapReduce) b).getMemoryKey());
    }

    public static <T> Comparator<T> chainComparators(final Comparator<T>[] comparators) {
        return Stream.of(comparators).reduce((a, b) -> a.thenComparing(b)).get();
    }

}
