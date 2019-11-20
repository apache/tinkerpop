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
package org.apache.tinkerpop.gremlin.process.computer;

import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 * The result of the {@link GraphComputer}'s computation. This is returned in a {@code Future} by
 * {@link GraphComputer#submit}. A GraphComputer computation yields two things: an updated view of the computed on
 * {@link Graph} and any computational sideEffects called {@link Memory}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface ComputerResult extends AutoCloseable {

    /**
     * Get the {@link Graph} computed as determined by {@link GraphComputer.Persist} and {@link GraphComputer.ResultGraph}.
     *
     * @return The computed graph
     */
    public Graph graph();

    /**
     * Get the GraphComputer's computational sideEffects known as {@link Memory}.
     *
     * @return the computed memory
     */
    public Memory memory();

    /**
     * Close the computed {@link GraphComputer} result. The semantics of "close" differ depending on the underlying implementation.
     * In general, when a {@link ComputerResult} is closed, the computed values are no longer available to the user.
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception;

}
