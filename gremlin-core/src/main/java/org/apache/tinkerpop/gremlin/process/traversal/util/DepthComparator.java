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
package org.apache.tinkerpop.gremlin.process.traversal.util;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;

import java.util.Comparator;

/**
 * A {@code Comparator} that compares steps on the depth of the traversal that they are in.
 */
public final class DepthComparator implements Comparator<Step<?,?>> {

    private static final DepthComparator instance = new DepthComparator();

    private DepthComparator() {}

    public static DepthComparator instance() {
        return instance;
    }

    @Override
    public int compare(final Step<?, ?> step1, final Step<?, ?> step2) {
        return getDepth(step2) - getDepth(step1);
    }

    private int getDepth(final Step<?,?> step) {
        return step == null || step instanceof EmptyStep ? 0 : getDepth((Step) step.getTraversal().getParent()) + 1;
    }
}
