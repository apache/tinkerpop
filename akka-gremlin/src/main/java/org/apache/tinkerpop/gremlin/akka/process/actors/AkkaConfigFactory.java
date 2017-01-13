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

import akka.actor.Address;
import akka.actor.AddressFromURIString;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.akka.process.actors.io.gryo.GryoSerializer;
import org.apache.tinkerpop.gremlin.process.actors.ActorProgram;
import org.apache.tinkerpop.gremlin.structure.Partition;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
final class AkkaConfigFactory {

    private AkkaConfigFactory() {
        // static method class
    }

    static Config generateAkkaConfig(final ActorProgram actorProgram, final Configuration configuration) {
        Config config = ConfigFactory.defaultApplication().
                withValue("akka.actor.serialization-bindings", ConfigValueFactory.fromMap(GryoSerializer.getSerializerBindings(configuration))).
                withValue("custom-dispatcher.mailbox-requirement", ConfigValueFactory.fromAnyRef(ActorMailbox.class.getCanonicalName() + "$" + ActorMailbox.ActorSemantics.class.getSimpleName())).
                withValue("custom-dispatcher-mailbox.mailbox-type", ConfigValueFactory.fromAnyRef(ActorMailbox.class.getCanonicalName())).
                withValue("akka.actor.mailbox.requirements", ConfigValueFactory.fromMap(Collections.singletonMap(ActorMailbox.class.getCanonicalName() + "$" + ActorMailbox.ActorSemantics.class.getSimpleName(), "custom-dispatcher-mailbox"))).
                withValue("custom-dispatcher-mailbox.message-priorities",
                        ConfigValueFactory.fromAnyRef(actorProgram.getMessagePriorities().
                                orElse(Collections.singletonList(Object.class)).
                                stream().
                                map(Class::getCanonicalName).
                                collect(Collectors.toList())));
        final Iterator<String> keys = configuration.getKeys();
        while (keys.hasNext()) {
            final String key = keys.next();
            try {
                final Object value = configuration.getProperty(key);
                config = config.withValue(key, ConfigValueFactory.fromAnyRef(convertValue(key, value)));
            } catch (final ConfigException.BugOrBroken e) {
                // do nothing -- basically, unserializable object
            }
        }
        return config;
    }

    private static Object convertValue(final String key, final Object value) {
        if (key.equals(Constants.AKKA_REMOTE_ENABLED_TRANSPORTS) || key.equals(Constants.AKKA_CLUSTER_SEED_NODES))
            return value instanceof Collection ? value : Collections.singletonList(value);
        else
            return value;
    }

    static Address getMasterActorDeployment(final Configuration configuration) {
        final String hostName = configuration.getString(Constants.AKKA_REMOTE_NETTY_TCP_HOSTNAME);
        final String port = configuration.getProperty(Constants.AKKA_REMOTE_NETTY_TCP_PORT).toString();
        return AddressFromURIString.parse("akka.tcp://" + configuration.getString(Constants.GREMLIN_AKKA_SYSTEM_NAME) + "@" + hostName + ":" + port);
    }

    static Address getWorkerActorDeployment(final Configuration configuration, final Partition partition) {
        final String hostName = partition.location().isSiteLocalAddress() ? "127.0.0.1" : partition.location().getHostAddress().toString();
        final String port = configuration.getProperty(Constants.AKKA_REMOTE_NETTY_TCP_PORT).toString();
        return AddressFromURIString.parse("akka.tcp://" + configuration.getString(Constants.GREMLIN_AKKA_SYSTEM_NAME) + "@" + hostName + ":" + port);
    }
}
