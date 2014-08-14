package com.tinkerpop.gremlin.tinkergraph.structure;

import com.tinkerpop.gremlin.structure.StructurePerformanceSuite;
import com.tinkerpop.gremlin.structure.StructureStandardSuite;
import com.tinkerpop.gremlin.tinkergraph.TinkerGraphGraphProvider;
import org.junit.runner.RunWith;

/**
 * Executes the Gremlin Structure Performance Test Suite using TinkerGraph.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(StructurePerformanceSuite.class)
@StructureStandardSuite.GraphProviderClass(provider = TinkerGraphGraphProvider.class, graph = TinkerGraph.class)
public class TinkerGraphStructurePerformanceTest {

}