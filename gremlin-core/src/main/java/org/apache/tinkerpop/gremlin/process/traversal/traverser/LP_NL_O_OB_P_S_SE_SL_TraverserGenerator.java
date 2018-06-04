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

package org.apache.tinkerpop.gremlin.process.traversal.traverser;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;

import java.util.EnumSet;
import java.util.Set;

public final class LP_NL_O_OB_P_S_SE_SL_TraverserGenerator implements TraverserGenerator {

    private static final Set<TraverserRequirement> REQUIREMENTS = EnumSet.of(
            TraverserRequirement.LABELED_PATH,
            TraverserRequirement.NESTED_LOOP,
            TraverserRequirement.OBJECT,
            TraverserRequirement.ONE_BULK,
            TraverserRequirement.PATH,
            TraverserRequirement.SACK,
            TraverserRequirement.SIDE_EFFECTS,
            TraverserRequirement.SINGLE_LOOP);


    private static final LP_NL_O_OB_P_S_SE_SL_TraverserGenerator INSTANCE = new LP_NL_O_OB_P_S_SE_SL_TraverserGenerator();

    private LP_NL_O_OB_P_S_SE_SL_TraverserGenerator() {
    }

    @Override
    public <S> Traverser.Admin<S> generate(final S start, final Step<S, ?> startStep, final long initialBulk) {
        return new LP_NL_O_OB_P_S_SE_SL_Traverser<>(start, startStep);
    }

    @Override
    public Set<TraverserRequirement> getProvidedRequirements() {
        return REQUIREMENTS;
    }

    public static LP_NL_O_OB_P_S_SE_SL_TraverserGenerator instance() {
        return INSTANCE;
    }
}