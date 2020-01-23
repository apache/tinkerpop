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
package org.apache.tinkerpop.gremlin.process.traversal.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.TokenTraversal;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface PathProcessor {

    public enum ElementRequirement {
        ID, LABEL, PROPERTIES, EDGES
    }

    public default ElementRequirement getMaxRequirement() {
        ElementRequirement max = ElementRequirement.ID;
        if (this instanceof TraversalParent) {
            for (final Traversal.Admin<Object, Object> traversal : ((TraversalParent) this).getLocalChildren()) {
                if (traversal instanceof IdentityTraversal) {
                    if (max.compareTo(ElementRequirement.ID) < 0)
                        max = ElementRequirement.ID;
                } else if (traversal instanceof TokenTraversal && ((TokenTraversal) traversal).getToken().equals(T.id)) {
                    if (max.compareTo(ElementRequirement.ID) < 0)
                        max = ElementRequirement.ID;
                } else if (traversal instanceof TokenTraversal && ((TokenTraversal) traversal).getToken().equals(T.label)) {
                    if (max.compareTo(ElementRequirement.LABEL) < 0)
                        max = ElementRequirement.LABEL;
                } else if (traversal instanceof ValueTraversal) {
                    if (max.compareTo(ElementRequirement.PROPERTIES) < 0)
                        max = ElementRequirement.PROPERTIES;
                } else {
                    max = ElementRequirement.EDGES;
                }
            }
        }
        return max;
    }

    public void setKeepLabels(final Set<String> keepLabels);

    public Set<String> getKeepLabels();

    public static <S> Traverser.Admin<S> processTraverserPathLabels(final Traverser.Admin<S> traverser, final Set<String> labels) {
        if (null != labels) traverser.keepLabels(labels);
        return traverser;
    }
}
