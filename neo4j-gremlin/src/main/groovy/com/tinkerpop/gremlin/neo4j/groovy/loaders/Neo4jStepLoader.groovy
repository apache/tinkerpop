package com.tinkerpop.gremlin.neo4j.groovy.loaders

import com.tinkerpop.gremlin.groovy.GFunction
import com.tinkerpop.gremlin.groovy.GSupplier
import com.tinkerpop.gremlin.groovy.loaders.GremlinLoader
import com.tinkerpop.gremlin.neo4j.process.graph.Neo4jTraversal
import com.tinkerpop.gremlin.process.graph.GraphTraversal
import com.tinkerpop.gremlin.util.function.SFunction

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class Neo4jStepLoader {

    public static void load() {

        Neo4jTraversal.metaClass.propertyMissing = { final String name ->
            if (GremlinLoader.isStep(name)) {
                return delegate."$name"();
            } else {
                return ((Neo4jTraversal) delegate).value(name);
            }
        }

        [Iterable, Iterator].each {
            it.metaClass.mean = {
                double counter = 0;
                double sum = 0;
                delegate.each { counter++; sum += it; }
                return sum / counter;
            }
        }

        Neo4jTraversal.metaClass.getAt = { final Integer index ->
            return ((Neo4jTraversal) delegate).range(index, index);
        }

        Neo4jTraversal.metaClass.getAt = { final Range range ->
            return ((Neo4jTraversal) delegate).range(range.getFrom() as Integer, range.getTo() as Integer);
        }

        // THE CODE BELOW IS REQUIRED UNTIL GROOVY 2.3+ FIXES VAR ARG CONVERSION OF CLOSURES TO LAMBDAS

        Neo4jTraversal.metaClass.path = { final Closure... pathClosures ->
            return ((Neo4jTraversal) delegate).path(GFunction.make(pathClosures));
        }

        Neo4jTraversal.metaClass.select = { final List<String> asLabels ->
            return ((Neo4jTraversal) delegate).select(asLabels, new SFunction[0]);
        }

        Neo4jTraversal.metaClass.select = { final List<String> asLabels, final Closure... stepClosures ->
            return ((Neo4jTraversal) delegate).select(asLabels, GFunction.make(stepClosures));
        }

        Neo4jTraversal.metaClass.select = { final Closure... stepClosures ->
            return ((Neo4jTraversal) delegate).select(GFunction.make(stepClosures));
        }

        Neo4jTraversal.metaClass.tree = { final Closure... branchClosures ->
            return ((Neo4jTraversal) delegate).tree(GFunction.make(branchClosures));
        }

        Neo4jTraversal.metaClass.pageRank { final Closure closure ->
            final Closure newClosure = closure.dehydrate();
            return ((Neo4jTraversal) delegate).pageRank(new GSupplier(newClosure));
        }
    }
}
