package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.process.computer.GraphComputer;
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

        // these are the classes that have Exceptions that need to be checked.
        final Class[] blueprintsExceptions = {
                AnnotatedValue.Exceptions.class,
                Graph.Annotations.Exceptions.class,
                Edge.Exceptions.class,
                Element.Exceptions.class,
                Graph.Exceptions.class,
                GraphComputer.Exceptions.class,
                Property.Exceptions.class,
                Transaction.Exceptions.class,
                Vertex.Exceptions.class
        };

        // this is a list of exceptions that are "excused" from the coverage tests.  in most cases there should only
        // be exceptions ignored if they are "base" exception methods that are used to compose other exceptions,
        // like the first set listed (and labelled as such) below in the list assignments.
        final Set<String> ignore = new HashSet<String>() {{
            // these exceptions is not used directly...they are called by other exception methods.
            add("com.tinkerpop.gremlin.structure.AnnotatedValue$Exceptions#annotationKeyIsReserved");
            add("com.tinkerpop.gremlin.structure.Property$Exceptions#propertyKeyIsReserved");
            add("com.tinkerpop.gremlin.structure.Graph$Annotations$Exceptions#graphAnnotationKeyIsReserved");

            // this is a general exception to be used as needed.  it is not explicitly tested:
            add("com.tinkerpop.gremlin.structure.Graph$Exceptions#argumentCanNotBeNull");

            // todo: need to write consistency tests for the following items still...........
            add("com.tinkerpop.gremlin.process.computer.GraphComputer$Exceptions#adjacentElementPropertiesCanNotBeRead");
            add("com.tinkerpop.gremlin.process.computer.GraphComputer$Exceptions#adjacentElementPropertiesCanNotBeWritten");
            add("com.tinkerpop.gremlin.process.computer.GraphComputer$Exceptions#constantComputeKeyHasAlreadyBeenSet");
            add("com.tinkerpop.gremlin.process.computer.GraphComputer$Exceptions#adjacentVerticesCanNotBeQueried");
            add("com.tinkerpop.gremlin.structure.Transaction$Exceptions#transactionMustBeOpenToReadWrite");
            add("com.tinkerpop.gremlin.structure.Transaction$Exceptions#openTransactionsOnClose");
        }};

        // implemented exceptions are the classes that potentially contains exception consistency checks.
        final Set<String> implementedExceptions = Stream.concat(Stream.of(ExceptionConsistencyTest.class.getDeclaredClasses()),
                Stream.of(FeatureSupportTest.class.getDeclaredClasses()))
                .<ExceptionCoverage>flatMap(c -> Stream.of(c.getAnnotationsByType(ExceptionCoverage.class)))
                .flatMap(ec -> Stream.of(ec.methods()).map(m -> String.format("%s#%s", ec.exceptionClass().getName(), m)))
                .collect(Collectors.<String>toSet());

        // evaluate each of the blueprints exceptions and find the list of exception methods...assert them against
        // the list of implementedExceptions to make sure they are covered.
        Stream.of(blueprintsExceptions).flatMap(c -> Stream.of(c.getDeclaredMethods()).map(m -> String.format("%s#%s", c.getName(), m.getName())))
                .filter(s -> !ignore.contains(s))
                .forEach(s -> {
                    System.out.println(s);
                    assertTrue(implementedExceptions.contains(s));
                });
    }
}
