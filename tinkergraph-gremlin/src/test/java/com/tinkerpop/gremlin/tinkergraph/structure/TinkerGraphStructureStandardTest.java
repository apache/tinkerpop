package com.tinkerpop.gremlin.tinkergraph.structure;

import com.tinkerpop.gremlin.structure.StructureStandardSuite;
import com.tinkerpop.gremlin.tinkergraph.TinkerGraphGraphProvider;
import org.junit.runner.RunWith;


/**
 * Executes the Standard Gremlin Structure Test Suite using TinkerGraph.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(StructureStandardSuite.class)
@StructureStandardSuite.GraphProviderClass(provider = TinkerGraphGraphProvider.class, graph = TinkerGraph.class)
public class TinkerGraphStructureStandardTest {

}
