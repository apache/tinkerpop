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
package org.apache.tinkerpop.gremlin.process.traversal;

import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.Map;

/**
 * Merge options relevant to upsert-like steps {@code mergeV()} and {@code mergeE()} that are applied to the relevant
 * {@code option()} modulator.
 */
public enum Merge {

    /**
     * Allows definition of the action to take when a merge operation ends up not matching the search criteria.
     * Typically, this event means that an {@link Element} will be created.
     *
     * @since 3.6.0
     */
    onCreate,

    /**
     * Allows definition of the action to take when a merge operation ends up successfully matching the search criteria.
     * Typically, this event means that the matched {@link Element} will be returned.
     *
     * @since 3.6.0
     */
    onMatch
}
