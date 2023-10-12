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
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.util.Objects;

/**
 * Add a custom {@code Matcher} class for {@link Path} objects that will ignore the path labels in the equality
 * comparison.
 */
class IsPathEqualToMatcher extends TypeSafeMatcher<Path> {

    private final Path expected;

    public IsPathEqualToMatcher(final Path expected) {
        this.expected = expected;
    }

    @Override
    protected boolean matchesSafely(final Path item) {
        if (expected.size() != item.size()) {
            return false;
        }

        for (int ix = 0; ix < expected.size(); ix++) {
            if (!Objects.equals(expected.get(ix), item.get(ix))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void describeTo(final Description description) {
        description.appendText(expected.toString());
    }
}
