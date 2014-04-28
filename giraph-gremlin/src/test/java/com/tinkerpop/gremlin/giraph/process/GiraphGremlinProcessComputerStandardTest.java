package com.tinkerpop.gremlin.giraph.process;

import com.tinkerpop.gremlin.giraph.GiraphGremlinGraphProvider;
import com.tinkerpop.gremlin.process.ProcessComputerStandardSuite;
import org.junit.runner.RunWith;

/**
 * Executes the Standard Gremlin Process Test Suite using TinkerGraph.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(ProcessComputerStandardSuite.class)
@ProcessComputerStandardSuite.GraphProviderClass(GiraphGremlinGraphProvider.class)
public class GiraphGremlinProcessComputerStandardTest {
}