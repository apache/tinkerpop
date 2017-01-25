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

package org.apache.tinkerpop.gremlin.akka.process.actors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class Constants {

    private Constants() {
        // static method class
    }

    public static final String AKKA_LOG_DEAD_LETTERS_DURING_SHUTDOWN = "akka.log-dead-letters-during-shutdown";
    public static final String AKKA_ACTOR_SERIALIZE_MESSAGES = "akka.actor.serialize-messages";
    public static final String AKKA_ACTOR_SERIALIZERS_GRYO = "akka.actor.serializers.gryo";
    public static final String AKKA_ACTOR_PROVIDER = "akka.actor.provider";
    public static final String AKKA_REMOTE_ENABLED_TRANSPORTS = "akka.remote.enabled-transports";
    public static final String AKKA_REMOTE_NETTY_TCP_HOSTNAME = "akka.remote.netty.tcp.hostname";
    public static final String AKKA_REMOTE_NETTY_TCP_PORT = "akka.remote.netty.tcp.port";
    public static final String AKKA_CLUSTER_SEED_NODES = "akka.cluster.seed-nodes";
    public static final String AKKA_CLUSTER_AUTO_DOWN_UNREACHABLE_AFTER = "akka.cluster.auto-down-unreachable-after";
    public static final String GREMLIN_AKKA_SYSTEM_NAME = "gremlin.akka.system-name";
    public static final String AKKA_ACTOR_SERIALIZATION_BINDINGS = "akka.actor.serialization-bindings";

}
