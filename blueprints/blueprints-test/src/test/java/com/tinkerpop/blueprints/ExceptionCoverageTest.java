package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.computer.GraphComputer;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertTrue;

/**
 * A set of tests that ensure that the {@link ExceptionConsistencyTest} covers all defined Blueprints exceptions.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ExceptionCoverageTest {
    @Test
    public void shouldCoverAllExceptionsInTests() {
        final Class[] blueprintsExceptions = {
                AnnotatedList.Exceptions.class,
                Annotations.Exceptions.class,
                Edge.Exceptions.class,
                Element.Exceptions.class,
                Graph.Exceptions.class,
                GraphComputer.Exceptions.class,
                Property.Exceptions.class,
                Transaction.Exceptions.class,
                Vertex.Exceptions.class
        };

        final Set<String> ignore = new HashSet<String>() {{
            // these exceptions is not used directly...they are called by other exception methods.
            add("com.tinkerpop.blueprints.Annotations$Exceptions#annotationKeyIsReserved");
            add("com.tinkerpop.blueprints.Property$Exceptions#propertyKeyIsReserved");

            // todo: review why these are not covered in the tests
            add("com.tinkerpop.blueprints.Annotations$Exceptions#dataTypeOfAnnotationValueNotSupported");
            add("com.tinkerpop.blueprints.Graph$Exceptions#transactionsNotSupported");
            add("com.tinkerpop.blueprints.Graph$Exceptions#graphComputerNotSupported");
            add("com.tinkerpop.blueprints.Graph$Exceptions#graphStrategyNotSupported");
            add("com.tinkerpop.blueprints.Graph$Exceptions#argumentCanNotBeNull");
            add("com.tinkerpop.blueprints.computer.GraphComputer$Exceptions#adjacentElementPropertiesCanNotBeRead");
            add("com.tinkerpop.blueprints.computer.GraphComputer$Exceptions#adjacentElementPropertiesCanNotBeWritten");
            add("com.tinkerpop.blueprints.computer.GraphComputer$Exceptions#constantComputeKeyHasAlreadyBeenSet");
            add("com.tinkerpop.blueprints.computer.GraphComputer$Exceptions#adjacentVerticesCanNotBeQueried");
            add("com.tinkerpop.blueprints.Property$Exceptions#dataTypeOfPropertyValueNotSupported");
            add("com.tinkerpop.blueprints.Transaction$Exceptions#transactionMustBeOpenToReadWrite");
            add("com.tinkerpop.blueprints.Transaction$Exceptions#openTransactionsOnClose");
            add("com.tinkerpop.blueprints.Vertex$Exceptions#userSuppliedIdsNotSupported"); // ???
        }};

        final Set<String> implementedExceptions = new HashSet<>();
        Stream.of(ExceptionConsistencyTest.class.getDeclaredClasses())
                .flatMap(c -> Stream.of(c.getAnnotationsByType(ExceptionCoverage.class)))
                .forEach(ec -> implementedExceptions.addAll(Stream.of(ec.methods()).map(m -> String.format("%s#%s", ec.exceptionClass().getName(), m)).collect(Collectors.<String>toList())));

        Stream.of(blueprintsExceptions).flatMap(c -> Stream.of(c.getDeclaredMethods()).map(m -> String.format("%s#%s", c.getName(), m.getName())))
                .filter(s->!ignore.contains(s))
                .forEach(s-> {
                    System.out.println(s);
                    assertTrue(implementedExceptions.contains(s));
                });
    }
}
