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

/**
 * A {@code GraphComputing} step is one that will change its behavior whether its on a {@link org.apache.tinkerpop.gremlin.process.computer.GraphComputer} or not.
 * {@link org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ComputerVerificationStrategy} is responsible for calling the {@link GraphComputing#onGraphComputer()} method.
 * This method is only called for global children steps of a {@link TraversalParent}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface GraphComputing {

    /**
     * The step will be executing on a {@link org.apache.tinkerpop.gremlin.process.computer.GraphComputer}.
     */
    public void onGraphComputer();

}
