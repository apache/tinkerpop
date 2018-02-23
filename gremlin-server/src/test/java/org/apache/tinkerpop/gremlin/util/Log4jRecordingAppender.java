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
package org.apache.tinkerpop.gremlin.util;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.LoggingEvent;

import java.util.ArrayList;
import java.util.List;

/**
 * Provides a way to gather logging events for purpose of testing log output.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Log4jRecordingAppender extends AppenderSkeleton {
    private final List<String> messages = new ArrayList<>();
    private final List<LoggingEvent> events = new ArrayList<>();

    public Log4jRecordingAppender() {
        super();
        setLayout(new PatternLayout("%p - %m%n"));
    }

    @Override
    protected void append(final LoggingEvent event) {
        messages.add(layout.format(event));
        events.add(event);
    }

    @Override
    public void close() {
    }

    @Override
    public boolean requiresLayout() {
        return true;
    }

    public List<String> getMessages() { return messages; }

    public List<LoggingEvent> getEvents() { return events; }

    public void clear() {
        messages.clear();
    }

    public boolean logContainsAny(final String regex) {
        // chop off the line feed so that the regex doesn't have to account for that
        return messages.stream().anyMatch(m -> m.substring(0,m.length() - 1).matches(regex));
    }

    public boolean logContainsAny(final String loggerName, final Level level, final String fragment) {
        return events.stream().anyMatch(m -> m.getLoggerName().equals(loggerName) &&
                m.getLevel().equals(level) && m.getMessage().toString().contains(fragment));
    }
    public boolean logMatchesAny(final String loggerName, final Level level, final String regex) {
        return events.stream().anyMatch(m -> m.getLoggerName().equals(loggerName) &&
                m.getLevel().equals(level) && m.getMessage().toString().matches(regex));
    }
}
