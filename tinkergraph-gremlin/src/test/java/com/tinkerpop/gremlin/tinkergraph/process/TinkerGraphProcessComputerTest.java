package com.tinkerpop.gremlin.tinkergraph.process;

import com.tinkerpop.gremlin.process.ProcessComputerSuite;
import com.tinkerpop.gremlin.tinkergraph.TinkerGraphGraphProvider;
import org.junit.runner.RunWith;

/**
 * Executes the Standard Gremlin Process Test Suite using TinkerGraph.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(ProcessComputerSuite.class)
@ProcessComputerSuite.GraphProviderClass(TinkerGraphGraphProvider.class)
public class TinkerGraphProcessComputerTest {
}
