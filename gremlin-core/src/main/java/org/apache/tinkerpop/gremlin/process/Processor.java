/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.process;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.ProcessorTraversalStrategy;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.concurrent.Future;

/**
 * This is a marker interface that denotes that the respective implementation is able to evaluate/execute/process a
 * {@link org.apache.tinkerpop.gremlin.process.traversal.Traversal}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Processor {

    /**
     * Get the {@link Configuration} that represents the current state of the Processor.
     * In other words, as the Processor is fluently built, the configuration should mutate
     * accordingly to capture all such changes.
     *
     * @return the configuration representing the current state of the processor
     */
    public Configuration configuration();

    /**
     * Every processor must be able to execute Gremlin {@link org.apache.tinkerpop.gremlin.process.traversal.Bytecode}.
     * In order to do this, there must be a {@link org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy} capable
     * of taking a traversal and configuring it to execute on the processor.
     *
     * @return the traversal strategy which will configure a traversal for execution on the processor
     */
    public ProcessorTraversalStrategy<? extends Processor> getProcessorTraversalStrategy();

    /**
     * Execute the Processor against the provided {@link Graph}.
     * The processor typically has a "program" associated with it that will execute some algorithm against the graph.
     *
     * @param graph the graph to execute the processor against
     * @return a future result
     */
    public Future submit(final Graph graph);

}
