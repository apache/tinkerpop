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

import org.apache.tinkerpop.gremlin.process.traversal.Pop;

import java.util.Set;

public interface PopContaining {
    public Set<PopInstruction> getPopInstructions();

    class PopInstruction {
        private final Pop pop;
        private final String label;

        public PopInstruction(final Pop pop, final String label) {
            this.pop = pop;
            this.label = label;
        }

        public Pop getPop() {
            return pop;
        }

        public String getLabel() {
            return label;
        }
    }
}
