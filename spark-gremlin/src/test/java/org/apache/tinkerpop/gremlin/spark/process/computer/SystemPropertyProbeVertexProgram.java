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
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collections;
import java.util.Set;

/**
 * A tiny test-only {@link VertexProgram} whose {@link #execute(Vertex, Messenger, Memory)} runs <b>on the executor</b>
 * and checks whether a given JVM system property is visible there with an expected value, reducing the observation to
 * the driver via a single boolean {@link Memory} key ({@link Operator#or}: {@code true} iff <i>any</i> executor saw
 * it). It lets a {@code local-cluster} test prove that a value injected into
 * {@code spark.executor.extraJavaOptions} genuinely crosses into a forked executor JVM — the sink the
 * {@code configure()} allow-list guards against untrusted OLAP input.
 * <p/>
 * The program is reconstructed on each executor by {@link VertexProgram#createVertexProgram(Graph, Configuration)}
 * (reflective no-arg constructor + {@link #loadState(Graph, Configuration)}), so its only state — the property name
 * and expected value — is carried in {@link #storeState(Configuration)}, not via Java serialization.
 */
public final class SystemPropertyProbeVertexProgram implements VertexProgram<Object> {

    private static final String CONFIG_PROPERTY_NAME = "gremlin.tierb.probe.propertyName";
    private static final String CONFIG_EXPECTED_VALUE = "gremlin.tierb.probe.expectedValue";

    /**
     * Boolean {@link Memory} key readable on the driver after the job: {@code true} iff at least one executor JVM
     * observed the probed system property with the expected value.
     */
    public static final String MEMORY_OBSERVED = "gremlin.tierb.probe.observedOnExecutor";

    private String propertyName;
    private String expectedValue;

    public static SystemPropertyProbeVertexProgram forProperty(final String propertyName, final String expectedValue) {
        final SystemPropertyProbeVertexProgram vp = new SystemPropertyProbeVertexProgram();
        vp.propertyName = propertyName;
        vp.expectedValue = expectedValue;
        return vp;
    }

    @Override
    public void storeState(final Configuration configuration) {
        VertexProgram.super.storeState(configuration);
        configuration.setProperty(CONFIG_PROPERTY_NAME, this.propertyName);
        configuration.setProperty(CONFIG_EXPECTED_VALUE, this.expectedValue);
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        this.propertyName = configuration.getString(CONFIG_PROPERTY_NAME);
        this.expectedValue = configuration.getString(CONFIG_EXPECTED_VALUE);
    }

    @Override
    public void setup(final Memory memory) {
        memory.set(MEMORY_OBSERVED, false);
    }

    @Override
    public void execute(final Vertex vertex, final Messenger<Object> messenger, final Memory memory) {
        // runs inside the (forked) executor JVM: report whether the injected -D reached this process
        memory.add(MEMORY_OBSERVED, this.expectedValue.equals(System.getProperty(this.propertyName)));
    }

    @Override
    public boolean terminate(final Memory memory) {
        return true; // a single iteration is enough to sample the executor JVM
    }

    @Override
    public Set<MemoryComputeKey> getMemoryComputeKeys() {
        return Collections.singleton(MemoryComputeKey.of(MEMORY_OBSERVED, Operator.or, true, false));
    }

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        return Collections.emptySet();
    }

    @Override
    @SuppressWarnings("CloneDoesntCallSuperClone")
    public SystemPropertyProbeVertexProgram clone() {
        return forProperty(this.propertyName, this.expectedValue);
    }

    @Override
    public GraphComputer.ResultGraph getPreferredResultGraph() {
        return GraphComputer.ResultGraph.ORIGINAL;
    }

    @Override
    public GraphComputer.Persist getPreferredPersist() {
        return GraphComputer.Persist.NOTHING;
    }
}
