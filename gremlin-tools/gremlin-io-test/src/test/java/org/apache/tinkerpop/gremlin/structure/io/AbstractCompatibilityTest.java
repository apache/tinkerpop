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
package org.apache.tinkerpop.gremlin.structure.io;

import static org.hamcrest.core.Is.is;
import static org.junit.Assume.assumeThat;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractCompatibilityTest {
    protected final Model model = Model.instance();

    public abstract Compatibility getCompatibility();

    protected void assumeCompatibility(final String resource) {
        final Model.Entry e = model.find(resource).orElseThrow(() -> new IllegalStateException("Could not find model"));
        assumeThat("Test model is not compatible with IO", e.isCompatibleWith(getCompatibility()), is(true));
    }

    protected <T> T findModelEntryObject(final String resourceName) {
        return model.find(resourceName).orElseThrow(() -> new IllegalStateException("Could not find requested model entry")).getObject();
    }
}
