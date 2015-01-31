package com.tinkerpop.gremlin.neo4j.process.groovy;

import com.tinkerpop.gremlin.neo4j.DefaultNeo4jGraphProvider;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import com.tinkerpop.gremlin.process.GroovyProcessStandardSuite;
import com.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.runner.RunWith;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(GroovyProcessStandardSuite.class)
@ProcessStandardSuite.GraphProviderClass(provider = DefaultNeo4jGraphProvider.class, graph = Neo4jGraph.class)
public class Neo4jGraphGroovyProcessStandardTest {
}
