package com.tinkerpop.gremlin.neo4j.structure;

import com.tinkerpop.gremlin.neo4j.Neo4jGraphProvider;
import com.tinkerpop.gremlin.process.ProcessComputerStandardSuite;
import org.junit.Ignore;
import org.junit.runner.RunWith;


/**
 * Executes the Standard Gremlin Structure Test Suite using Neo4j.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(ProcessComputerStandardSuite.class)
@Ignore
@ProcessComputerStandardSuite.GraphProviderClass(Neo4jGraphProvider.class)
public class Neo4jGraphProcessComputerStandardTest {
}
