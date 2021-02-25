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

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Log4jRecordingAppenderTest {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Log4jRecordingAppenderTest.class);
    private Log4jRecordingAppender recordingAppender = null;
    private static final String lineSeparator = System.getProperty("line.separator");

    private Level originalConfiguredLevel = null;

    @Before
    public void setupForEachTest() {
        recordingAppender = new Log4jRecordingAppender();
        final Logger rootLogger = Logger.getRootLogger();
        if (null == originalConfiguredLevel) originalConfiguredLevel = rootLogger.getLevel();
        rootLogger.addAppender(recordingAppender);
        rootLogger.setLevel(Level.ALL);

        logger.error("ERROR");
        logger.warn("WARN");
        logger.info("INFO");
    }

    @After
    public void teardownForEachTest() {
        final Logger rootLogger = Logger.getRootLogger();
        rootLogger.removeAppender(recordingAppender);
        rootLogger.setLevel(originalConfiguredLevel);
    }

    @Test
    public void shouldRecordMessages() {
        assertEquals(3, recordingAppender.getMessages().size());
        assertEquals("ERROR - ERROR" + lineSeparator, recordingAppender.getMessages().get(0));
        assertEquals("WARN - WARN"  + lineSeparator, recordingAppender.getMessages().get(1));
        assertEquals("INFO - INFO" + lineSeparator, recordingAppender.getMessages().get(2));
    }

    @Test
    public void shouldMatchAnyMessages() {
        assertThat(recordingAppender.logContainsAny("ERROR.*"), is(true));
    }

    @Test
    public void shouldMatchNoMessages() {
        assertThat(recordingAppender.logContainsAny("this is not here"), is(false));
    }

    @Test
    public void shouldClearMessages() {
        assertEquals(3, recordingAppender.getMessages().size());
        recordingAppender.clear();
        assertEquals(0, recordingAppender.getMessages().size());
    }
}
