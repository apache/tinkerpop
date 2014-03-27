package com.tinkerpop.gremlin.neo4j.process;

import com.tinkerpop.gremlin.neo4j.Neo4jGraphProvider;
import com.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.runner.RunWith;


/**
 * Executes the Standard Gremlin Structure Test Suite using Neo4j.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(ProcessStandardSuite.class)
@ProcessStandardSuite.GraphProviderClass(Neo4jGraphProvider.class)
public class Neo4jGraphProcessStandardTest {
}
