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
package org.apache.tinkerpop.gremlin.structure.util.detached;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class DetachedUtil {

    private DetachedUtil() {}

    /**
     * Provides a way to mutate something that is "detached". This method is really for internal usage as there
     * typically is not need for application developers to mutate a "detached" element.
     */
    public static void addProperty(final DetachedElement e, final DetachedProperty p) {
        e.internalAddProperty(p);
    }
    public static void addProperty(final DetachedVertex v, final DetachedVertexProperty vp) {
        v.internalAddProperty(vp);
    }

    public static void setId(final DetachedElement e, final Object id) {
        e.internalSetId(id);
    }

    public static void setLabel(final DetachedElement e, final String label) {
        e.inernalSetLabel(label);
    }

    public static void setValue(final DetachedVertexProperty vp, final Object value) {
        vp.internalSetValue(value);
    }

    public static DetachedVertex newDetachedVertex() {
        return new DetachedVertex();
    }

    public static DetachedVertexProperty newDetachedVertexProperty() {
        return new DetachedVertexProperty();
    }

    public static DetachedEdge newDetachedEdge() {
        return new DetachedEdge();
    }

    public static void setInV(final DetachedEdge e, final DetachedVertex v) {
        e.internalSetInV(v);
    }

    public static void setOutV(final DetachedEdge e, final DetachedVertex v) {
        e.internalSetOutV(v);
    }
}
