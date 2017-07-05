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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;

import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class RequirementsStep<S> extends AbstractStep<S, S> {

    private final Set<TraverserRequirement> requirements;

    public RequirementsStep(final Traversal.Admin traversal, final Set<TraverserRequirement> requirements) {
        super(traversal);
        this.requirements = new HashSet<>(requirements);
    }

    public void addRequirement(final TraverserRequirement requirement) {
        this.requirements.add(requirement);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.requirements;
    }

    @Override
    protected Traverser.Admin<S> processNextStart() throws NoSuchElementException {
        return this.starts.next();
    }
}
