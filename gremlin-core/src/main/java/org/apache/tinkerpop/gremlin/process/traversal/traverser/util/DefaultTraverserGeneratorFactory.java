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
package org.apache.tinkerpop.gremlin.process.traversal.traverser.util;

import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_O_P_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_NL_O_P_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_O_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_NL_O_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_NL_O_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.LP_O_OB_P_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.LP_O_OB_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.O_OB_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.NL_O_OB_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.LP_NL_O_OB_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.LP_NL_O_OB_P_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserGeneratorFactory;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;

import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultTraverserGeneratorFactory implements TraverserGeneratorFactory {

    private static DefaultTraverserGeneratorFactory INSTANCE = new DefaultTraverserGeneratorFactory();

    public static DefaultTraverserGeneratorFactory instance() {
        return INSTANCE;
    }

    private DefaultTraverserGeneratorFactory() {
    }

    @Override
    public TraverserGenerator getTraverserGenerator(final Set<TraverserRequirement> requirements) {
        if (requirements.contains(TraverserRequirement.ONE_BULK)) {
            if (O_OB_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return O_OB_S_SE_SL_TraverserGenerator.instance();

            if (NL_O_OB_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return NL_O_OB_S_SE_SL_TraverserGenerator.instance();

            if (LP_O_OB_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return LP_O_OB_S_SE_SL_TraverserGenerator.instance();

            if (LP_NL_O_OB_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return LP_NL_O_OB_S_SE_SL_TraverserGenerator.instance();

            if (LP_O_OB_P_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return LP_O_OB_P_S_SE_SL_TraverserGenerator.instance();

            if (LP_NL_O_OB_P_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return LP_NL_O_OB_P_S_SE_SL_TraverserGenerator.instance();
        } else {
            if (B_O_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return B_O_TraverserGenerator.instance();

            if (B_O_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return B_O_S_SE_SL_TraverserGenerator.instance();

            if (B_NL_O_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return B_NL_O_S_SE_SL_TraverserGenerator.instance();

            if (B_LP_O_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return B_LP_O_S_SE_SL_TraverserGenerator.instance();

            if (B_LP_NL_O_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return B_LP_NL_O_S_SE_SL_TraverserGenerator.instance();

            if (B_LP_O_P_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return B_LP_O_P_S_SE_SL_TraverserGenerator.instance();

            if (B_LP_NL_O_P_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return B_LP_NL_O_P_S_SE_SL_TraverserGenerator.instance();
        }

        throw new IllegalStateException("The provided traverser generator factory does not support the requirements of the traversal: " + this.getClass().getCanonicalName() + requirements);
    }
}
