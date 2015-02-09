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
package com.tinkerpop.gremlin.process.graph.traversal.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.traversal.step.Reversible;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ExceptStep<S> extends FilterStep<S> implements Reversible {

    private final String sideEffectKeyOrPathLabel;

    public ExceptStep(final Traversal.Admin traversal, final String sideEffectKeyOrPathLabel) {
        super(traversal);
        this.sideEffectKeyOrPathLabel = sideEffectKeyOrPathLabel;
        this.setPredicate(traverser -> {
            final Object except = traverser.asAdmin().getSideEffects().exists(this.sideEffectKeyOrPathLabel) ?
                    traverser.sideEffects(this.sideEffectKeyOrPathLabel) :
                    traverser.path(this.sideEffectKeyOrPathLabel);
            return except instanceof Collection ?
                    !((Collection) except).contains(traverser.get()) :
                    !except.equals(traverser.get());
        });
    }

    public ExceptStep(final Traversal.Admin traversal, final Collection<S> exceptionCollection) {
        super(traversal);
        this.sideEffectKeyOrPathLabel = null;
        this.setPredicate(traverser -> !exceptionCollection.contains(traverser.get()));
    }

    public ExceptStep(final Traversal.Admin traversal, final S exceptionObject) {
        super(traversal);
        this.sideEffectKeyOrPathLabel = null;
        this.setPredicate(traverser -> !exceptionObject.equals(traverser.get()));
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, this.sideEffectKeyOrPathLabel);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return null == this.sideEffectKeyOrPathLabel ?
                Collections.singleton(TraverserRequirement.OBJECT) :
                Stream.of(TraverserRequirement.OBJECT,
                        TraverserRequirement.SIDE_EFFECTS,
                        TraverserRequirement.PATH_ACCESS).collect(Collectors.toSet());
    }
}
