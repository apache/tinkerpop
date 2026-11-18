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

/*
 * gremlin-shaded has no sources of its own, so this descriptor is not compiled by maven-compiler-plugin. It is added
 * to the shaded jar after maven-shade-plugin runs (see moditect-maven-plugin in gremlin-shaded/pom.xml). It exports
 * every relocated package, so the list below must be kept in sync with the shaded jar's packages whenever a bundled
 * dependency is upgraded (the package of the jackson-core fast-double-parser, for example, carries the Jackson
 * version in its name). ShadedArtifactVerificationTest fails if the two drift apart.
 */
module org.apache.tinkerpop.gremlin.shaded {
    // Kryo and Objenesis use sun.misc.Unsafe
    requires jdk.unsupported;
    requires java.logging;

    // optional jackson-databind and kryo integrations with these JDK modules, mirroring jackson-databind's own
    // descriptor
    requires static java.sql;
    requires static java.desktop;
    requires static java.xml;

    exports org.apache.tinkerpop.shaded.jackson.annotation;
    exports org.apache.tinkerpop.shaded.jackson.core;
    exports org.apache.tinkerpop.shaded.jackson.core.async;
    exports org.apache.tinkerpop.shaded.jackson.core.base;
    exports org.apache.tinkerpop.shaded.jackson.core.exc;
    exports org.apache.tinkerpop.shaded.jackson.core.filter;
    exports org.apache.tinkerpop.shaded.jackson.core.format;
    exports org.apache.tinkerpop.shaded.jackson.core.internal.shaded.fdp.v2_22_4;
    exports org.apache.tinkerpop.shaded.jackson.core.internal.shaded.fdp.v2_22_4.bte;
    exports org.apache.tinkerpop.shaded.jackson.core.internal.shaded.fdp.v2_22_4.chr;
    exports org.apache.tinkerpop.shaded.jackson.core.io;
    exports org.apache.tinkerpop.shaded.jackson.core.io.schubfach;
    exports org.apache.tinkerpop.shaded.jackson.core.json;
    exports org.apache.tinkerpop.shaded.jackson.core.json.async;
    exports org.apache.tinkerpop.shaded.jackson.core.sym;
    exports org.apache.tinkerpop.shaded.jackson.core.type;
    exports org.apache.tinkerpop.shaded.jackson.core.util;
    exports org.apache.tinkerpop.shaded.jackson.databind;
    exports org.apache.tinkerpop.shaded.jackson.databind.annotation;
    exports org.apache.tinkerpop.shaded.jackson.databind.cfg;
    exports org.apache.tinkerpop.shaded.jackson.databind.deser;
    exports org.apache.tinkerpop.shaded.jackson.databind.deser.impl;
    exports org.apache.tinkerpop.shaded.jackson.databind.deser.std;
    exports org.apache.tinkerpop.shaded.jackson.databind.exc;
    exports org.apache.tinkerpop.shaded.jackson.databind.ext;
    exports org.apache.tinkerpop.shaded.jackson.databind.introspect;
    exports org.apache.tinkerpop.shaded.jackson.databind.jdk14;
    exports org.apache.tinkerpop.shaded.jackson.databind.json;
    exports org.apache.tinkerpop.shaded.jackson.databind.jsonFormatVisitors;
    exports org.apache.tinkerpop.shaded.jackson.databind.jsonschema;
    exports org.apache.tinkerpop.shaded.jackson.databind.jsontype;
    exports org.apache.tinkerpop.shaded.jackson.databind.jsontype.impl;
    exports org.apache.tinkerpop.shaded.jackson.databind.module;
    exports org.apache.tinkerpop.shaded.jackson.databind.node;
    exports org.apache.tinkerpop.shaded.jackson.databind.ser;
    exports org.apache.tinkerpop.shaded.jackson.databind.ser.impl;
    exports org.apache.tinkerpop.shaded.jackson.databind.ser.std;
    exports org.apache.tinkerpop.shaded.jackson.databind.type;
    exports org.apache.tinkerpop.shaded.jackson.databind.util;
    exports org.apache.tinkerpop.shaded.jackson.databind.util.internal;
    exports org.apache.tinkerpop.shaded.kryo;
    exports org.apache.tinkerpop.shaded.kryo.factories;
    exports org.apache.tinkerpop.shaded.kryo.io;
    exports org.apache.tinkerpop.shaded.kryo.pool;
    exports org.apache.tinkerpop.shaded.kryo.serializers;
    exports org.apache.tinkerpop.shaded.kryo.shaded.org.objectweb.asm;
    exports org.apache.tinkerpop.shaded.kryo.util;
    exports org.apache.tinkerpop.shaded.minlog;
    exports org.apache.tinkerpop.shaded.objenesis;
    exports org.apache.tinkerpop.shaded.objenesis.instantiator;
    exports org.apache.tinkerpop.shaded.objenesis.instantiator.android;
    exports org.apache.tinkerpop.shaded.objenesis.instantiator.annotations;
    exports org.apache.tinkerpop.shaded.objenesis.instantiator.basic;
    exports org.apache.tinkerpop.shaded.objenesis.instantiator.gcj;
    exports org.apache.tinkerpop.shaded.objenesis.instantiator.perc;
    exports org.apache.tinkerpop.shaded.objenesis.instantiator.sun;
    exports org.apache.tinkerpop.shaded.objenesis.instantiator.util;
    exports org.apache.tinkerpop.shaded.objenesis.strategy;

    // ObjectMapper.findModules() discovers Jackson modules with ServiceLoader
    uses org.apache.tinkerpop.shaded.jackson.databind.Module;
}
