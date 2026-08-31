/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.spark.process.computer;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.features.TestFiles;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoOutputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.script.ScriptInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.util.OlapClassLoadingPolicy;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.process.computer.clone.CloneVertexProgram;
import org.apache.tinkerpop.gremlin.spark.AbstractSparkTest;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Validates the OLAP IO trust boundary on a real (local) SparkGraphComputer job, using the config-driven
 * reader/writer + {@code graph.compute().program()} execution shape (not the {@code io()} step, which is guard-blocked
 * in this build):
 * <ul>
 *   <li>standard Gryo config-driven OLAP is unaffected;</li>
 *   <li>a {@code ScriptInputFormat} job is blocked unless {@code gremlin.io.trusted} is set;</li>
 *   <li>the trust flag propagates through the job configuration to the record reader, so a trusted script job runs.</li>
 * </ul>
 * Local mode runs executors in-JVM; cross-JVM configuration serialization is covered by
 * {@link SparkLocalClusterIoSecurityIntegrateTest}.
 */
public class SparkHadoopIoSecurityIntegrateTest extends AbstractSparkTest {

    private static final String SCRIPT_FILE = "gremlin.hadoop.scriptInputFormat.script";

    private String writeModernInputGraph() throws Exception {
        final String input = TestHelper.makeTestDataFile(SparkHadoopIoSecurityIntegrateTest.class,
                UUID.randomUUID().toString(), "input.kryo");
        final TinkerGraph modern = TinkerFactory.createModern();
        modern.io(IoCore.gryo()).writeGraph(input);
        modern.close();
        return input;
    }

    private static long countCauseChainFor(final Throwable t, final String marker) {
        for (Throwable c = t; c != null; c = c.getCause())
            if (c.getMessage() != null && c.getMessage().contains(marker)) return 1;
        return 0;
    }

    // baseline Gryo config-driven OLAP traversal -- must be unaffected by the restriction.
    @Test
    public void shouldRunBuiltInGryoOlapUnaffected() throws Exception {
        final String input = writeModernInputGraph();
        final Configuration configuration = getBaseConfiguration();
        configuration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, input);
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GryoInputFormat.class.getCanonicalName());
        final Graph graph = GraphFactory.open(configuration);
        assertEquals(6L, graph.traversal().withComputer(SparkGraphComputer.class).V().count().next().longValue());
    }

    // config-driven CloneVertexProgram Gryo->Gryo IO -- the config path HadoopIoStep never touches; unaffected.
    @Test
    public void shouldCloneWithBuiltInFormatsUnaffected() throws Exception {
        final String input = writeModernInputGraph();
        final String output = TestHelper.makeTestDataDirectory(SparkHadoopIoSecurityIntegrateTest.class, UUID.randomUUID().toString());
        final Configuration configuration = getBaseConfiguration();
        configuration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, input);
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GryoInputFormat.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, GryoOutputFormat.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, output);
        final Graph graph = GraphFactory.open(configuration);
        graph.compute(SparkGraphComputer.class).program(CloneVertexProgram.build().create()).submit().get();
        // read the output back to prove the round-trip completed
        final Configuration readBack = getBaseConfiguration();
        readBack.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, output);
        readBack.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GryoInputFormat.class.getCanonicalName());
        assertEquals(6L, GraphFactory.open(readBack).traversal().withComputer(SparkGraphComputer.class).V().count().next().longValue());
    }

    // ScriptInputFormat job WITHOUT the trust flag -- must be blocked at the (in-JVM) executor record reader.
    @Test
    public void shouldBlockScriptInputFormatWhenUntrusted() {
        final Configuration configuration = scriptInputConfiguration(false);
        try {
            GraphFactory.open(configuration).traversal().withComputer(SparkGraphComputer.class).V().count().next();
            fail("a ScriptInputFormat OLAP job must be blocked when gremlin.io.trusted is not set");
        } catch (final Throwable t) {
            assertTrue("rejection must name the trust flag; was: " + t,
                    countCauseChainFor(t, OlapClassLoadingPolicy.TRUSTED) > 0);
        }
    }

    // ScriptInputFormat job WITH gremlin.io.trusted=true -- must run, proving the flag reaches the reader.
    @Test
    public void shouldAllowScriptInputFormatWhenTrusted() {
        final Configuration configuration = scriptInputConfiguration(true);
        final long count = GraphFactory.open(configuration).traversal().withComputer(SparkGraphComputer.class).V().count().next();
        assertTrue("a trusted ScriptInputFormat job should load vertices; got " + count, count > 0);
    }

    // Propagation micro-check: the trust flag survives the graph-config -> Hadoop-config transform SparkGraphComputer uses.
    @Test
    public void shouldPropagateTrustFlagThroughConfUtil() {
        final Configuration configuration = scriptInputConfiguration(true);
        final org.apache.hadoop.conf.Configuration hadoop = ConfUtil.makeHadoopConfiguration(configuration);
        assertTrue("gremlin.io.trusted must survive ConfUtil into the Hadoop Configuration shipped to executors",
                hadoop.getBoolean(OlapClassLoadingPolicy.TRUSTED, false));
    }

    private Configuration scriptInputConfiguration(final boolean trusted) {
        final Configuration configuration = getBaseConfiguration();
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, ScriptInputFormat.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, TestFiles.PATHS.get("grateful-dead.txt"));
        configuration.setProperty(SCRIPT_FILE, TestFiles.PATHS.get("script-input-grateful-dead.groovy"));
        if (trusted)
            configuration.setProperty(OlapClassLoadingPolicy.TRUSTED, true);
        return configuration;
    }
}
