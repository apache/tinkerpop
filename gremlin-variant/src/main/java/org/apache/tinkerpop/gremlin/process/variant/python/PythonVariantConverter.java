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

package org.apache.tinkerpop.gremlin.process.variant.python;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.variant.VariantConverter;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PythonVariantConverter implements VariantConverter {

    private static final Set<String> PREFIX_NAMES = new HashSet<>(Arrays.asList("as", "in", "and", "or", "is", "not", "from"));

    @Override
    public void step(final StringBuilder currentTraversal, final String stepName, final Object... arguments) {
        if (arguments.length == 0)
            currentTraversal.append(".").append(convertStepName(stepName)).append("()");
        else {
            String temp = "." + convertStepName(stepName) + "(";
            for (final Object object : arguments) {
                if (object instanceof Object[]) {
                    for (final Object object2 : (Object[]) object) {
                        temp = temp + convertToString(object2) + ",";
                    }
                } else {
                    temp = temp + convertToString(object) + ",";
                }
            }
            currentTraversal.append(temp.substring(0, temp.length() - 1) + ")");
        }
    }

    private static String convertToString(final Object object) {
        if (object instanceof String)
            return "\"" + object + "\"";
        else if (object instanceof Direction)
            return "\"Direction." + object.toString() + "\"";
        else if (object instanceof P)
            return "\"P." + object.toString() + "\"";
        else if (object instanceof T)
            return "\"T." + object.toString() + "\"";
        else
            return object.toString();
    }

    private static String convertStepName(final String stepName) {
        if (PREFIX_NAMES.contains(stepName))
            return "_" + stepName;
        else
            return stepName;
    }

}
