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
/**
 * Abstracts a minimal subset of Kryo types and methods.
 * <p>
 * Kryo is often shaded.  For instance, TinkerPop's Gryo
 * serializer relies on a shaded Kryo package.
 * TinkerPop serializers written against a particular shaded
 * Kryo package (or an unshaded Kryo package) are compatible
 * only with that package.  In contrast, TinkerPop serializers written
 * against this abstraction can be used with any shaded or
 * unshaded Kryo package, so long as the signatures and behavior
 * of the methods in this package remain stable.
 * <p>
 * To show how this is useful, consider
 * {@link org.apache.tinkerpop.gremlin.structure.util.star.StarGraphSerializer}.
 * This class has logic unique to TinkerPop that performs
 * efficient and forward-compatible serialization of
 * {@link org.apache.tinkerpop.gremlin.structure.util.star.StarGraph}
 * instances.  It takes advantage of package-level visibility
 * and the fact that it shares a package with its target,
 * so it would be challenging to cleanly and naturally replicate
 * (i.e. without package spoofing or runtime visibility overrides).
 * By implementing
 * {@link org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.SerializerShim}
 * instead of, say, Gryo's shaded
 * {@link org.apache.tinkerpop.shaded.kryo.Serializer},
 * such a serializer can be used with anybody's Kryo package,
 * regardless of whether
 * that package is shaded or not.  This lets third-parties reuse
 * TinkerPop's efficient, internals-aware StarGraph serializer on
 * their own serialization platform (and without altering
 * TinkerPop's bytecode, let alone its source).
 * <p>
 * The number of types and methods in this
 * package is deliberately small to reduce the likelihood of a
 * new Kryo release introducing an incompatible change.
 */
package org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim;