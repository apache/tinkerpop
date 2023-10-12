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
package org.apache.tinkerpop.gremlin.features;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;

import java.util.Arrays;
import java.util.Collection;

/**
 * Customer matcher than mimics the behavior of the Hamcrest {@code IsIn} but handles {@link Path} using our custom
 * {@link IsPathEqualToMatcher} to ignore labels.
 */
class IsInMatcher<T> extends BaseMatcher<T> {
    private final Collection<T> collection;

    public IsInMatcher(final Collection<T> collection) {
        this.collection = collection;
    }

    public IsInMatcher(final T[] elements) {
        this.collection = Arrays.asList(elements);
    }

    public boolean matches(final Object o) {
        // introduce some custom handling for Path since we don't really want to assert the labels
        if (o instanceof Path) {
            return this.collection.stream().anyMatch(c -> new IsPathEqualToMatcher((Path) o).matchesSafely((Path) c));
        } else {
            return this.collection.contains(o);
        }
    }

    public void describeTo(final Description buffer) {
        buffer.appendText("one of ");
        buffer.appendValueList("{", ", ", "}", this.collection);
    }
}
