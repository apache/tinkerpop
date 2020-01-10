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
package org.apache.tinkerpop.gremlin.driver;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.mockito.ArgumentMatcher;
import org.mockito.internal.matchers.apachecommons.ReflectionEquals;

/**
 * Adapts a Mockito {@code ArgumentMatcher} to a Hamcrest {@code Matcher} so that it can be used with the Hamcrest
 * {@code assertThat}.
 */
public class MockitoHamcrestMatcherAdapter extends BaseMatcher {

    private final ArgumentMatcher<Object> mockitoMatcher;

    public MockitoHamcrestMatcherAdapter(final ArgumentMatcher<Object> mockitoMatcher) {
        this.mockitoMatcher = mockitoMatcher;
    }

    public static MockitoHamcrestMatcherAdapter reflectionEquals(final Object wanted, final String... excludeFields) {
        return new MockitoHamcrestMatcherAdapter(new ReflectionEquals(wanted, excludeFields));
    }

    @Override
    public boolean matches(final Object o) {
        return mockitoMatcher.matches(o);
    }

    @Override
    public void describeTo(final Description description) {
        description.appendValue(mockitoMatcher.toString());
    }
}