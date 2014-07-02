package com.tinkerpop.gremlin.giraph.process;

import com.tinkerpop.gremlin.giraph.GiraphGraphProvider;
import com.tinkerpop.gremlin.process.ProcessComputerStandardSuite;
import org.junit.Ignore;
import org.junit.runner.RunWith;

/**
 * Executes the Standard Gremlin Process Test Suite using GiraphGraph.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(ProcessComputerStandardSuite.class)
@ProcessComputerStandardSuite.GraphProviderClass(GiraphGraphProvider.class)
@Ignore
public class GiraphProcessComputerStandardTest {
}