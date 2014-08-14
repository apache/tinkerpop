package com.tinkerpop.gremlin.neo4j.process;

import com.tinkerpop.gremlin.neo4j.Neo4jGraphProvider;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import com.tinkerpop.gremlin.process.ProcessComputerSuite;
import org.junit.Ignore;
import org.junit.runner.RunWith;


/**
 * Executes the Standard Gremlin Structure Test Suite using Neo4j.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(ProcessComputerSuite.class)
@Ignore // todo: why ignored and not dealt with through features or OptOut???
@ProcessComputerSuite.GraphProviderClass(provider = Neo4jGraphProvider.class, graph = Neo4jGraph.class)
public class Neo4jGraphProcessComputerTest {
}
