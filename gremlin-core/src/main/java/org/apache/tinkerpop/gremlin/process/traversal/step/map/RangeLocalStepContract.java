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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.Ranging;

import java.util.List;

/**
 * Defines the contract for {@code range} related steps.
 */
public interface RangeLocalStepContract<S> extends Step<S, S>, Ranging {

    /**
     * Concrete implementations of this contract that can be referenced as TinkerPop implementations.
     */
    List<Class<? extends Step>> CONCRETE_STEPS = List.of(RangeLocalStep.class, RangeLocalStepPlaceholder.class);

    /**
     * Retrieves the lower bound of the range.
     *
     * @return the value representing the lower bound of the range
     */
    Long getLowRange();

    /**
     * Retrieves the higher bound of the range.
     *
     * @return the higher bound of the range as an object of type V
     */
    Long getHighRange();

    /**
     * getLowRange, retaining the GValue container and without pinning the variable. It is the caller's
     * responsibility to ensure that this value is not used to alter the traversal in any way which is not generalizable
     * to any parameter value.
     * @return the lower bound for range().
     */
    default GValue<Long> getLowRangeAsGValue() {
        return GValue.of(getLowRange());
    }

    /**
     * getHighRange, retaining the GValue container and without pinning the variable. It is the caller's
     * responsibility to ensure that this value is not used to alter the traversal in any way which is not generalizable
     * to any parameter value.
     * @return the upper bound for range().
     */
    default GValue<Long> getHighRangeAsGValue() {
        return GValue.of(getHighRange());
    }
}
