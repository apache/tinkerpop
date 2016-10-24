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
package org.apache.tinkerpop.gremlin.process.remote.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalSideEffects;

/**
 * When a traversal is executed remotely, the ability to retrieve those side-effects (i.e. the value in "a" in the
 * traversal {@code g.V().aggregate('a').values('name')}) can be exposed through this interface. As an example,
 * with TinkerPop's {@code DriverRemoteConnection} that connects to Gremlin Server as a "remote", the side-effects
 * are left on the server. The {@code RemoteTraversalSideEffects} implementation, in that case, lazily loads those
 * side-effects when requested. Implementations should attempt to match the features of the locally processed
 * {@link DefaultTraversalSideEffects} to keep consistency.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface RemoteTraversalSideEffects extends TraversalSideEffects, AutoCloseable {

    /**
     * If the "remote" that actually executed the traversal maintained resources that can be released, when the user
     * is done with the side-effects, then this method can be used to trigger that release.
     */
    @Override
    public default void close() throws Exception {
        //  do nothing
    }
}
