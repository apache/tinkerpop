package com.tinkerpop.gremlin.neo4j.structure;

import com.tinkerpop.gremlin.neo4j.Neo4jGraphProvider;
import com.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.runner.RunWith;


/**
 * Executes the Standard Gremlin Structure Test Suite using Neo4j.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(StructureStandardSuite.class)
@StructureStandardSuite.GraphProviderClass(provider = Neo4jGraphProvider.class, graph = Neo4jGraph.class)
public class Neo4jGraphStructureStandardTest {
}
