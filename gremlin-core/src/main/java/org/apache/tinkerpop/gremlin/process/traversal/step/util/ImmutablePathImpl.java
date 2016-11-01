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
package org.apache.tinkerpop.gremlin.process.traversal.step.util;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;

import java.util.List;

/**
 * Internal interface used by ImmutablePath to provide more efficient implementation.
 *
 * @author Matt Frantz (http://github.com/mhfrantz)
 * @deprecated Since 3.2.3 ({@link ImmutablePath} contains all requisite behavior)
 */
@Deprecated
interface ImmutablePathImpl extends Path {

    /**
     * Get the object least recently associated with the particular label of the path.
     *
     * @param label the label of the path
     * @param <A>   the type of the object associated with the label
     * @return the object associated with the label of the path or null if the path does not contain the label
     */
    public <A> A getSingleHead(final String label);

    /**
     * Get the object most recently associated with the particular label of the path.
     *
     * @param label the label of the path
     * @param <A>   the type of the object associated with the label
     * @return the object associated with the label of the path or null if the path does not contain the label
     */
    public <A> A getSingleTail(final String label);
}
