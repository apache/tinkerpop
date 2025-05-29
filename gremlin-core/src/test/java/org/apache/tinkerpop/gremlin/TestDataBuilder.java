/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.tinkerpop.gremlin;

import java.util.HashSet;

import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.step.PopContaining;

/**
 * This class is responsible for building test data for `PopInstruction` Unit tests.
 * It provides methods to create a set of `PopInstruction` test data objects.
 *
 * @author Vaibhav Malhotra
 */
public class TestDataBuilder {

    // Helper function to create a Set of `PopInstruction` values
    public static HashSet<PopContaining.PopInstruction> createPopInstructionSet(Object[]... pairs) {
        HashSet<PopContaining.PopInstruction> popInstructions = new HashSet<>();

        // Each pair should contain a name (String) and a Pop value
        for (Object[] pair : pairs) {
            if (pair.length == 2 && pair[0] instanceof String && pair[1] instanceof Pop) {
                popInstructions.add(new PopContaining.PopInstruction((String)pair[0], (Pop)pair[1]));
            }
        }

        return popInstructions;
    }
}
