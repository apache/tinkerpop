/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueHolder;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class AddVertexStartStepPlaceholder extends AbstractAddVertexStepPlaceholder<Vertex>
        implements AddVertexStepContract<Vertex>, GValueHolder<Vertex, Vertex> {

    public AddVertexStartStepPlaceholder(final Traversal.Admin traversal, final String label) {
        super(traversal, label);
    }

    public AddVertexStartStepPlaceholder(final Traversal.Admin traversal, final GValue<String> label) {
        super(traversal, label);
    }

    public AddVertexStartStepPlaceholder(final Traversal.Admin traversal, final Traversal.Admin<?,?> vertexLabelTraversal) {
        super(traversal, vertexLabelTraversal == null ? null : (Traversal.Admin<Vertex,?>) vertexLabelTraversal);
    }

    public AddVertexStartStepPlaceholder(final Traversal.Admin traversal, final Collection<Object> labels) {
        super(traversal, labels);
    }

    @Override
    public AddVertexStartStep asConcreteStep() {
        AddVertexStartStep step;
        if (label instanceof Collection) {
            final Set<String> resolvedLabels = new LinkedHashSet<>();
            final List<Traversal.Admin<?, ?>> labelTraversals = new ArrayList<>();
            for (final Object l : (Collection<?>) label) {
                if (l instanceof Traversal) {
                    labelTraversals.add(((Traversal<?, ?>) l).asAdmin());
                } else if (l instanceof GValue) {
                    resolvedLabels.add((String) ((GValue<?>) l).get());
                } else {
                    resolvedLabels.add((String) l);
                }
            }
            step = labelTraversals.isEmpty() ? new AddVertexStartStep(traversal, resolvedLabels)
                    : new AddVertexStartStep(traversal, labelTraversals);
        } else if (label instanceof Traversal) {
            step = new AddVertexStartStep(traversal, ((Traversal<?, ?>) label).asAdmin());
        } else if (label instanceof GValue) {
            step = new AddVertexStartStep(traversal, ((GValue<String>) label).get());
        } else {
            // When userProvidedLabel is false, label may be the default from the placeholder
            // hierarchy. Pass null so AddVertexStartStep does not inject T.label.
            final String labelStr = hasUserProvidedLabel() ? (String) label : null;
            step = new AddVertexStartStep(traversal, labelStr);
        }
        super.configureConcreteStep(step);
        return step;
    }
}
