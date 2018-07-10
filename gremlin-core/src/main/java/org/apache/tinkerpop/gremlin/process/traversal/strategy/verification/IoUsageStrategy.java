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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.verification;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NoneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ReadStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.WriteStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;

import java.util.Collections;
import java.util.Set;

/**
 * {@code IoUsageStrategy} prevents the {@link GraphTraversalSource#read(String)} and
 * {@link GraphTraversalSource#write(String)} steps from being used outside of their intended scope, which is as the
 * first and last step in a traversal. Therefore, it can only be used as {@code g.read('file.gryo')} and
 * {@code g.write('file.gryo')}. As both of these steps take additional configuration, the use of the
 * {@link GraphTraversal#with(String, Object)} is acceptable.
 * <p/>
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @example <pre>
 * g.read('file.kryo').V()            // throws VerificationException
 * g.write('file.kryo').V()           // throws VerificationException
 * </pre>
 */
public final class IoUsageStrategy extends AbstractTraversalStrategy<TraversalStrategy.VerificationStrategy> implements TraversalStrategy.VerificationStrategy {

    private static final IoUsageStrategy INSTANCE = new IoUsageStrategy();

    private IoUsageStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        final Step start = traversal.getStartStep();
        final Step end = traversal.getEndStep();
        if ((start instanceof ReadStep || start instanceof WriteStep) && end != start) {
            if ((end instanceof NoneStep && traversal.getSteps().size() > 2)) {
                throw new VerificationException("The read() or write() steps must be the first and only step in the traversal - they cannot be used with other steps", traversal);
            }
        }
    }

    public static IoUsageStrategy instance() {
        return INSTANCE;
    }

    @Override
    public Set<Class<? extends VerificationStrategy>> applyPrior() {
        return Collections.singleton(ComputerVerificationStrategy.class);
    }
}
