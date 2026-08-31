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
package org.apache.tinkerpop.gremlin.hadoop.process.computer.traversal.step.sideEffect;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONOutputFormat;
import org.apache.tinkerpop.gremlin.process.traversal.IO;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.ReadWriting;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.util.OlapClassLoadingPolicy;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.util.OlapConfigKeyPolicy;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HadoopIoStepTest {

    private static HadoopIoStep readStepWith(final String key, final Object value) {
        final HadoopIoStep step = new HadoopIoStep(__.start().asAdmin(), "graph.kryo");
        step.setMode(ReadWriting.Mode.READING);
        step.configure(key, value);
        return step;
    }

    private static HadoopIoStep writeStepWith(final String key, final Object value) {
        final HadoopIoStep step = new HadoopIoStep(__.start().asAdmin(), "output");
        step.setMode(ReadWriting.Mode.WRITING);
        step.configure(key, value);
        return step;
    }

    // A raw graphReader key set via io().with() is denied by the default config-key policy: it is neither a built-in
    // nor an operator-approved key. There is no class-value-specific gate here and the value never reaches class
    // loading -- this simply pins that the internal graphReader key stays out of the default allow-list. (The reader
    // FQCN supplied via the IO.reader token is gated separately; see shouldRejectReaderClassNameFromUntrustedTraversal.)
    @Test
    public void shouldDenyGraphReaderKeyByDefaultWhenUntrusted() {
        final HadoopGraph graph = HadoopGraph.open(new BaseConfiguration());
        final HadoopIoStep step = readStepWith(Constants.GREMLIN_HADOOP_GRAPH_READER, "evil.Reader");
        try {
            step.generateProgram(graph, null);
            fail("a raw graphReader key from a remote traversal must be denied by default in untrusted mode");
        } catch (final IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains(OlapClassLoadingPolicy.TRUSTED));
        }
    }

    // graphFilter is denied by the same default config-key policy because it is not a built-in/approved key. This is
    // NOT a graphFilter-deserialization guard: the value is never deserialized here, it is rejected at the key gate
    // before use (deserialization hardening of graphFilter is a separate concern). The test pins that graphFilter
    // stays out of the default allow-list, so an untrusted traversal cannot inject one.
    @Test
    public void shouldDenyGraphFilterKeyByDefaultWhenUntrusted() {
        final HadoopGraph graph = HadoopGraph.open(new BaseConfiguration());
        final HadoopIoStep step = readStepWith(Constants.GREMLIN_HADOOP_GRAPH_FILTER, "rO0ABXNy"); // never deserialized -- rejected at the key gate
        try {
            step.generateProgram(graph, null);
            fail("graphFilter is not an approved io() key, so it must be denied for an untrusted traversal");
        } catch (final IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains(OlapClassLoadingPolicy.TRUSTED));
        }
    }

    @Test
    public void shouldRejectSelfElevationFromUntrustedTraversal() {
        final HadoopGraph graph = HadoopGraph.open(new BaseConfiguration());
        final HadoopIoStep step = readStepWith(OlapClassLoadingPolicy.TRUSTED, true);
        try {
            step.generateProgram(graph, null);
            fail("a remote traversal must not be able to set the IO trust flag on itself");
        } catch (final IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains(OlapClassLoadingPolicy.TRUSTED));
        }
    }

    @Test
    public void shouldAllowClassValuedOptionInTrustedConfiguration() {
        final Configuration config = new BaseConfiguration();
        config.setProperty(OlapClassLoadingPolicy.TRUSTED, true);
        final HadoopGraph graph = HadoopGraph.open(config);
        final HadoopIoStep step = readStepWith(Constants.GREMLIN_HADOOP_GRAPH_READER, "my.custom.Reader");
        step.generateProgram(graph, null); // must not throw in trusted mode
        assertEquals("my.custom.Reader", graph.configuration().getString(Constants.GREMLIN_HADOOP_GRAPH_READER));
    }

    @Test
    public void shouldAcceptAnyReaderClassNameWhenTrusted() {
        // a trusted deployment restores the previous behavior: an arbitrary reader FQCN via IO.reader is accepted
        final Configuration config = new BaseConfiguration();
        config.setProperty(OlapClassLoadingPolicy.TRUSTED, true);
        final HadoopGraph graph = HadoopGraph.open(config);
        final HadoopIoStep step = readStepWith(IO.reader, "my.custom.Reader");
        step.generateProgram(graph, null); // must not throw in trusted mode
        assertEquals("my.custom.Reader", graph.configuration().getString(Constants.GREMLIN_HADOOP_GRAPH_READER));
    }

    @Test
    public void shouldAcceptAnyWriterClassNameWhenTrusted() {
        // a trusted deployment restores the previous behavior: an arbitrary writer FQCN via IO.writer is accepted
        final Configuration config = new BaseConfiguration();
        config.setProperty(OlapClassLoadingPolicy.TRUSTED, true);
        final HadoopGraph graph = HadoopGraph.open(config);
        final HadoopIoStep step = writeStepWith(IO.writer, "my.custom.Writer");
        step.generateProgram(graph, null); // must not throw in trusted mode
        assertEquals("my.custom.Writer", graph.configuration().getString(Constants.GREMLIN_HADOOP_GRAPH_WRITER));
    }

    @Test
    public void shouldRejectUnapprovedOptionFromUntrustedTraversal() {
        // untrusted io().with() permits only operator-approved keys; an unapproved key is rejected
        final HadoopGraph graph = HadoopGraph.open(new BaseConfiguration());
        final HadoopIoStep step = readStepWith("spark.executor.memory", "2g");
        try {
            step.generateProgram(graph, null);
            fail("a remote traversal must not set an unapproved OLAP configuration option in untrusted mode");
        } catch (final IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains(OlapClassLoadingPolicy.TRUSTED));
        }
    }

    @Test
    public void shouldAcceptApprovedGraphConfigKeyFromUntrustedTraversal() {
        // io().with() permits keys the operator approved via its own list (separate from the computer list)
        final Configuration config = new BaseConfiguration();
        config.setProperty(OlapConfigKeyPolicy.APPROVED_GRAPH_CONFIG_KEYS, "my.graph.option");
        final HadoopGraph graph = HadoopGraph.open(config);
        final HadoopIoStep step = readStepWith("my.graph.option", "v");
        step.generateProgram(graph, null); // approved for the io() surface -> copied
        assertEquals("v", graph.configuration().getString("my.graph.option"));
    }

    @Test
    public void shouldRejectApprovedGraphConfigKeysMetaKeyFromUntrustedTraversal() {
        // the approved-key list itself is a trust-boundary meta-key and is never settable via io().with()
        final HadoopGraph graph = HadoopGraph.open(new BaseConfiguration());
        final HadoopIoStep step = readStepWith(OlapConfigKeyPolicy.APPROVED_GRAPH_CONFIG_KEYS, "attacker.key");
        try {
            step.generateProgram(graph, null);
            fail("the approved-graph-config-keys meta-key must not be settable via io().with()");
        } catch (final IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains(OlapConfigKeyPolicy.APPROVED_GRAPH_CONFIG_KEYS));
        }
    }

    @Test
    public void shouldRejectReaderClassNameFromUntrustedTraversal() {
        // an FQCN supplied via the IO.reader token must not bypass the approved-format guard
        final HadoopGraph graph = HadoopGraph.open(new BaseConfiguration());
        final HadoopIoStep step = readStepWith(IO.reader, "org.attacker.Rce");
        try {
            step.generateProgram(graph, null);
            fail("a reader FQCN from a remote traversal must be rejected in untrusted mode");
        } catch (final IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains(OlapClassLoadingPolicy.APPROVED_CLASSES));
        }
    }

    @Test
    public void shouldAcceptReaderKeywordFromUntrustedTraversal() {
        final HadoopGraph graph = HadoopGraph.open(new BaseConfiguration());
        final HadoopIoStep step = readStepWith(IO.reader, IO.graphson);
        step.generateProgram(graph, null); // a built-in keyword is always allowed
        assertEquals(GraphSONInputFormat.class.getName(),
                graph.configuration().getString(Constants.GREMLIN_HADOOP_GRAPH_READER));
    }

    @Test
    public void shouldAcceptApprovedReaderClassNameFromUntrustedTraversal() {
        final Configuration config = new BaseConfiguration();
        config.setProperty(OlapClassLoadingPolicy.APPROVED_CLASSES, "org.provider.CustomInputFormat");
        final HadoopGraph graph = HadoopGraph.open(config);
        final HadoopIoStep step = readStepWith(IO.reader, "org.provider.CustomInputFormat");
        step.generateProgram(graph, null); // approved via the operator's approvedClasses list
        assertEquals("org.provider.CustomInputFormat",
                graph.configuration().getString(Constants.GREMLIN_HADOOP_GRAPH_READER));
    }

    // The write path (configureForWrite) resolves the writer format symmetrically to the read path; the following
    // mirror the reader FQCN/keyword/approved cases for the writer side.

    @Test
    public void shouldRejectWriterClassNameFromUntrustedTraversal() {
        final HadoopGraph graph = HadoopGraph.open(new BaseConfiguration());
        final HadoopIoStep step = writeStepWith(IO.writer, "org.attacker.Rce");
        try {
            step.generateProgram(graph, null);
            fail("a writer FQCN from a remote traversal must be rejected in untrusted mode");
        } catch (final IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains(OlapClassLoadingPolicy.APPROVED_CLASSES));
        }
    }

    @Test
    public void shouldAcceptWriterKeywordFromUntrustedTraversal() {
        final HadoopGraph graph = HadoopGraph.open(new BaseConfiguration());
        final HadoopIoStep step = writeStepWith(IO.writer, IO.graphson);
        step.generateProgram(graph, null); // a built-in keyword is always allowed
        assertEquals(GraphSONOutputFormat.class.getName(),
                graph.configuration().getString(Constants.GREMLIN_HADOOP_GRAPH_WRITER));
    }

    @Test
    public void shouldAcceptApprovedWriterClassNameFromUntrustedTraversal() {
        final Configuration config = new BaseConfiguration();
        config.setProperty(OlapClassLoadingPolicy.APPROVED_CLASSES, "org.provider.CustomOutputFormat");
        final HadoopGraph graph = HadoopGraph.open(config);
        final HadoopIoStep step = writeStepWith(IO.writer, "org.provider.CustomOutputFormat");
        step.generateProgram(graph, null); // approved via the operator's approvedClasses list
        assertEquals("org.provider.CustomOutputFormat",
                graph.configuration().getString(Constants.GREMLIN_HADOOP_GRAPH_WRITER));
    }
}
