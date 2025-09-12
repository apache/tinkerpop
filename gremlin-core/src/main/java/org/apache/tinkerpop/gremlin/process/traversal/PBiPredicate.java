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

import java.util.function.BiPredicate;

/**
 * Marker interface for predefined {@code BiPredicate} predicates that can be used in {@code Predicate}}.
 *
 * Allows to set the name of the predicate that will be used for serialization.
 * Restricts the use of random {@code BiPredicate} with {@link P}.
 **/
public interface PBiPredicate<T, U> extends BiPredicate<T, U> {

    /**
     * Gets predicate name that can be used for serialization.
     **/
    default String getPredicateName() {
        return toString();
    }

}
