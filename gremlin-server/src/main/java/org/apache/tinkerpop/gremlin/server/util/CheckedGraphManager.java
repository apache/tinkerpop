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
package org.apache.tinkerpop.gremlin.server.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.shaded.minlog.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link GraphManager} that will prevent Gremlin Server from starting if all configured graphs fail.
 */
public class CheckedGraphManager extends DefaultGraphManager {

    private static final Logger logger = LoggerFactory.getLogger(CheckedGraphManager.class);

    private List<StartupFailure> startupFailures;

    public CheckedGraphManager(final Settings settings) {
        super(settings);
        if (getGraphNames().isEmpty()) {
            if (getStartupFailures().isEmpty()) {
                throw new IllegalStateException("No graph configured in settings:" + settings.graphs);
            } else if (startupFailures.size() == 1) {
                StartupFailure failure = startupFailures.get(0);
                throw new IllegalStateException(failure.toString(), failure.exception);
            } else {
                throw new IllegalStateException(
                        "All " + startupFailures.size() + " graphs has failed:" + startupFailures,
                        startupFailures.get(0).exception);
            }
        }
    }

    private List<StartupFailure> getStartupFailures() {
        if (startupFailures == null) {
            startupFailures = new ArrayList<>();
        }
        return startupFailures;
    }

    @Override
    protected void addGraph(final String name, final String configurationFile) {
        try {
            final Graph newGraph = GraphFactory.open(configurationFile);
            putGraph(name, newGraph);
            logger.info("Graph [{}] was successfully configured via [{}].", name, configurationFile);
        } catch (Throwable e) {
            final StartupFailure failure = new StartupFailure(name, configurationFile, e);
            if (logger.isDebugEnabled()) {
                Log.debug(failure.toString(), e);
            }
            getStartupFailures().add(failure);
        }
    }

    private static class StartupFailure {
        private final String name;
        private final String configurationFile;
        private final Throwable exception;

        public StartupFailure(final String name, final String configurationFile, final Throwable exception) {
            this.name = name;
            this.configurationFile = configurationFile;
            this.exception = exception;
        }

        public String toString() {
            return String.format(
                    "Graph [%s] configured at [%s] could not be instantiated and will not be available in Gremlin Server.  GraphFactory message: %s",
                    name, configurationFile, exception.getMessage());
        }
    }
}
