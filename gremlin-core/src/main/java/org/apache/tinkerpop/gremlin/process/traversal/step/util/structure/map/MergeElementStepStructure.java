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
package org.apache.tinkerpop.gremlin.process.traversal.step.util.structure.map;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

import java.util.Map;

public interface MergeElementStepStructure<S> {
    /**
     * Gets the traversal that will be used to provide the {@code Map} that will be used to search for elements.
     * This {@code Map} also will be used as the default data set to be used to create the element if the search is not
     * successful.
     */
    public Traversal.Admin<S, Map> getMergeTraversal();

    /**
     * Gets the traversal that will be used to provide the {@code Map} that will be used to create elements that
     * do not match the search criteria of {@link #getMergeTraversal()}.
     */
    public Traversal.Admin<S, Map> getOnCreateTraversal();

    /**
     * Gets the traversal that will be used to provide the {@code Map} that will be used to modify elements that
     * match the search criteria of {@link #getMergeTraversal()}.
     */
    public Traversal.Admin<S, Map> getOnMatchTraversal();
}
