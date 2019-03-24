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
package org.apache.tinkerpop.machine.traverser.path;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Path extends Serializable, Cloneable {

    public enum Pop {
        first, last, all;
    }

    public void add(final String label, final Object object);

    public Object object(final int index);

    public String label(final int index);

    public Object get(final Pop pop, final String label);

    public int size();

    public Path clone();

    public static class Exceptions {

        private Exceptions() {
        }

        public static IllegalArgumentException noObjectsForLabel(final String label) {
            return new IllegalArgumentException("The path does not have an object for the provided label: " + label);
        }
    }
}
