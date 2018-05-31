/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.traversal.step;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;

/**
 * Identifies a {@link Step} as one that can accept configurations via the {@link GraphTraversal#with(String, Object)}
 * step modulator. The nature of the configuration allowed is specific to the implementation.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Configuring extends Parameterizing {

    /**
     * Accept a configuration to the {@link Step}. Note that this interface extends {@link Parameterizing} and so
     * there is an expectation that the {@link Step} implementation will have a {@link Parameters} instance that will
     * house any values passed to this method. Storing these configurations in {@link Parameters} is not a requirement
     * however, IF the configuration is an expected option for the step and can be stored on a member field that can
     * be accessed on the step by more direct means (i.e. like a getter method).
     */
    public void configure(final Object... keyValues);
}
