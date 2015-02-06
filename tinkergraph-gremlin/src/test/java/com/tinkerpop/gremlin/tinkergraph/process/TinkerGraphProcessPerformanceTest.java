package com.tinkerpop.gremlin.tinkergraph.process;

import com.tinkerpop.gremlin.process.ProcessPerformanceSuite;
import com.tinkerpop.gremlin.process.ProcessStandardSuite;
import com.tinkerpop.gremlin.tinkergraph.TinkerGraphGraphProvider;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.runner.RunWith;

/**
 * Executes the Performance Gremlin Process Test Suite using TinkerGraph.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(ProcessPerformanceSuite.class)
@ProcessStandardSuite.GraphProviderClass(provider = TinkerGraphGraphProvider.class, graph = TinkerGraph.class)
public class TinkerGraphProcessPerformanceTest {
}
