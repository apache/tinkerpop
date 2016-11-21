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
package org.apache.tinkerpop.gremlin.jsr223.console;

import java.io.Closeable;
import java.util.List;

/**
 * The Gremlin Console supports the {@code :remote} and {@code :submit} commands which provide standardized ways
 * for plugins to provide "remote connections" to resources and a way to "submit" a command to those resources.
 * A "remote connection" does not necessarily have to be a remote server.  It simply refers to a resource that is
 * external to the console.
 * <p/>
 * By implementing this interface and returning an instance of it through {@link ConsoleCustomizer#getRemoteAcceptor()}
 * a plugin can hook into those commands and provide remoting features.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface RemoteAcceptor extends Closeable {

    public static final String RESULT = "result";

    /**
     * Gets called when {@code :remote} is used in conjunction with the "connect" option.  It is up to the
     * implementation to decide how additional arguments on the line should be treated after "connect".
     *
     * @return an object to display as output to the user
     * @throws RemoteException if there is a problem with connecting
     */
    public Object connect(final List<String> args) throws RemoteException;

    /**
     * Gets called when {@code :remote} is used in conjunction with the {@code config} option.  It is up to the
     * implementation to decide how additional arguments on the line should be treated after {@code config}.
     *
     * @return an object to display as output to the user
     * @throws RemoteException if there is a problem with configuration
     */
    public Object configure(final List<String> args) throws RemoteException;

    /**
     * Gets called when {@code :submit} is executed.  It is up to the implementation to decide how additional
     * arguments on the line should be treated after {@code :submit}.
     *
     * @return an object to display as output to the user
     * @throws RemoteException if there is a problem with submission
     */
    public Object submit(final List<String> args) throws RemoteException;

    /**
     * If the {@code RemoteAcceptor} is used in the Gremlin Console, then this method might be called to determine
     * if it can be used in a fashion that supports the {@code :remote console} command.  By default, this value is
     * set to {@code false}.
     * <p/>
     * A {@code RemoteAcceptor} should only return {@code true} for this method if it expects that all activities it
     * supports are executed through the {@code :sumbit} command. If the users interaction with the remote requires
     * working with both local and remote evaluation at the same time, it is likely best to keep this method return
     * {@code false}. A good example of this type of plugin would be the Gephi Plugin which uses {@code :remote config}
     * to configure a local {@code TraversalSource} to be used and expects calls to {@code :submit} for the same body
     * of analysis.
     */
    public default boolean allowRemoteConsole() {
        return false;
    }
}
