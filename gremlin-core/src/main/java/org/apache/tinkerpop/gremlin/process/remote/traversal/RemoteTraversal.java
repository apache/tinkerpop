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

import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.remote.traversal.step.map.RemoteStep;
import org.apache.tinkerpop.gremlin.process.traversal.GremlinLang;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;

/**
 * A {@link RemoteTraversal} is returned from {@link RemoteConnection#submitAsync(GremlinLang)}. It is iterated from
 * within {@link RemoteStep} using {@link #nextTraverser()}. Implementations should typically be given a "result" from
 * a remote source where the traversal was executed. The "result" should be an iterator which preferably has its data
 * bulked.
 * <p/>
 * Note that internally {@link #nextTraverser()} is called from within a loop (specifically in
 * {@link AbstractStep#next()} that breaks properly when a {@code NoSuchElementException} is thrown. In other
 * words the "results" should be iterated to force that failure.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface RemoteTraversal<S,E> extends Traversal.Admin<S,E> {
}
