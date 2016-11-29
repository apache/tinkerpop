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
package org.apache.tinkerpop.gremlin.groovy.jsr223.customizer;

import groovy.transform.TimedInterrupt;
import org.apache.tinkerpop.gremlin.groovy.CompilerCustomizerProvider;
import org.apache.tinkerpop.gremlin.jsr223.Customizer;
import org.codehaus.groovy.ast.tools.GeneralUtils;
import org.codehaus.groovy.control.customizers.ASTTransformationCustomizer;
import org.codehaus.groovy.control.customizers.CompilationCustomizer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Injects a check in loops and other areas of code to interrupt script execution if the run time exceeds the
 * specified time.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TimedInterruptCustomizerProvider implements CompilerCustomizerProvider {
    public static final long DEFAULT_INTERRUPTION_TIMEOUT = 60000;

    private final long interruptionTimeout;

    public TimedInterruptCustomizerProvider() {
        this(DEFAULT_INTERRUPTION_TIMEOUT);
    }

    public TimedInterruptCustomizerProvider(final Long interruptionTimeout) {
        this.interruptionTimeout = interruptionTimeout;
    }

    public TimedInterruptCustomizerProvider(final Integer interruptionTimeout) {
        this.interruptionTimeout = interruptionTimeout.longValue();
    }

    @Override
    public CompilationCustomizer create() {
        final Map<String, Object> timedInterruptAnnotationParams = new HashMap<>();
        timedInterruptAnnotationParams.put("value", interruptionTimeout);
        timedInterruptAnnotationParams.put("unit", GeneralUtils.propX(GeneralUtils.classX(TimeUnit.class), TimeUnit.MILLISECONDS.toString()));
        timedInterruptAnnotationParams.put("checkOnMethodStart", false);
        timedInterruptAnnotationParams.put("thrown", GeneralUtils.classX(TimedInterruptTimeoutException.class));
        return new ASTTransformationCustomizer(timedInterruptAnnotationParams, TimedInterrupt.class);
    }
}
