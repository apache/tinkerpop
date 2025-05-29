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
package org.apache.tinkerpop.gremlin.process.traversal.step;

import java.util.HashSet;
import java.util.Objects;

import org.apache.tinkerpop.gremlin.process.traversal.Pop;

/**
 * The {@code PopContaining} interface is implemented by traversal steps that maintain Pop instructions
 * for label access. It provides a mechanism to track and manage how labeled elements should
 * be accessed using {@link Pop} semantics (first, last, all, or mixed).
 *
 * In Gremlin traversals, various elements can be labeled and later accessed via these labels.
 * The {@link Pop} enum determines how to access these labeled elements when there are multiple
 * values associated with the same label.
 *
 * <pre>
 * {@code
 * // Simple example with default Pop.last behavior
 * gremlin> g.V().as("a").out().as("a").select("a")
 * ==>[v[2]]  // returns the last element labeled "a"
 *
 * // Using Pop.first to get the first labeled element
 * gremlin> g.V().as("a").out().as("a").select(first, "a")
 * ==>[v[1]]  // returns the first element labeled "a"
 *
 * // Using Pop.all to get all labeled elements
 * gremlin> g.V().as("a").out().as("a").select(all, "a")
 * ==>[v[1], v[2]]  // returns all elements labeled "a"
 * }
 * </pre>
 *
 * Steps implementing this interface maintain a collection of {@link PopInstruction} objects, each containing
 * a label and a {@link Pop} value that specifies how to access elements with that label.
 *
 * @author Vaibhav Malhotra
 */
public interface PopContaining {
    public default HashSet<PopInstruction> getPopInstructions() {
        return new HashSet<PopInstruction>();
    }

        /**
         * A class for storing the Scope Context. It has two elements:
         * - label: String
         * - pop: Pop value
         */
        class PopInstruction {
            public Pop pop;
            public String label;

            public PopInstruction(String label, Pop pop) {
                this.pop = pop;
                this.label = label;
            }

            public PopInstruction(Pop pop, String label) {
                this.pop = pop;
                this.label = label;
            }

            public PopInstruction() {
               this.pop = null;
               this.label = "";
            }

        public String getLabel() {
                return label;
            }

            public Pop getPop() {
                return pop;
            }

            @Override
            public boolean equals(Object o) {
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                final PopInstruction that = (PopInstruction) o;
                return getPop() == that.getPop() && Objects.equals(getLabel(), that.getLabel());
            }

            @Override
            public int hashCode() {
                return Objects.hash(getPop(), getLabel());
            }
        }
}
