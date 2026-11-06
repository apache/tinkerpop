/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.fluency;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * One agent configuration in a {@link Matrix}. The model, harness and context are labels recorded with the results
 * so they can be compared. The driver does the actual configuring.
 */
public final class Setup {
    private static final Pattern NAME = Pattern.compile("[A-Za-z0-9._-]+");

    private final String name;
    private final String model;
    private final String harness;
    private final List<String> context;
    private final boolean server;
    private final List<String> command;
    private final Map<String, String> environment;
    private final String builtin;
    private final Path replayFile;

    Setup(final String name, final String model, final String harness, final List<String> context,
          final boolean server, final List<String> command, final Map<String, String> environment,
          final String builtin, final Path replayFile) {
        if (null == name || !NAME.matcher(name).matches())
            throw new IllegalArgumentException("Setup name must be letters, digits, '.', '_' or '-': " + name);
        if ((null == builtin) == command.isEmpty())
            throw new IllegalArgumentException("Setup " + name + " needs exactly one of driver command or builtin");
        if (builtin != null && !builtin.equals("reference") && !builtin.equals("replay"))
            throw new IllegalArgumentException("Setup " + name + " has unknown builtin driver: " + builtin);
        if ("replay".equals(builtin) && null == replayFile)
            throw new IllegalArgumentException("Setup " + name + " uses the replay driver without a file");

        this.name = name;
        this.model = model;
        this.harness = harness;
        this.context = Collections.unmodifiableList(new ArrayList<>(context));
        this.server = server;
        this.command = Collections.unmodifiableList(new ArrayList<>(command));
        this.environment = Collections.unmodifiableMap(new HashMap<>(environment));
        this.builtin = builtin;
        this.replayFile = replayFile;
    }

    public String getName() {
        return name;
    }

    public String getModel() {
        return model;
    }

    public String getHarness() {
        return harness;
    }

    public List<String> getContext() {
        return context;
    }

    /**
     * Whether the setup's agent queries the test graphs through a Gremlin Server.
     */
    public boolean usesServer() {
        return server;
    }

    public List<String> getCommand() {
        return command;
    }

    public String getBuiltin() {
        return builtin;
    }

    /**
     * @param endpoints maps a graph name to where the run's server serves it, or {@code null} if the run has none
     */
    Driver createDriver(final Duration driverTimeout, final Function<String, String> endpoints) throws IOException {
        if (server && null == endpoints)
            throw new IllegalStateException("Setup " + name + " needs a Gremlin Server but the run has none");

        if ("reference".equals(builtin)) return new ReferenceDriver();
        if ("replay".equals(builtin)) return ReplayDriver.fromFile(replayFile);
        return new CommandDriver(command, environment, driverTimeout, server ? endpoints : null);
    }
}
