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
package org.apache.tinkerpop.gremlin.process;

import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.VerificationException;
import org.junit.internal.AssumptionViolatedException;
import org.junit.internal.runners.model.EachTestNotifier;
import org.junit.runner.Description;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.NotSerializableException;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class GremlinProcessRunner extends BlockJUnit4ClassRunner {
    private static final Logger logger = LoggerFactory.getLogger(GremlinProcessRunner.class);
    public GremlinProcessRunner(Class<?> klass) throws InitializationError {
        super(klass);
    }

    @Override
    public void runChild(final FrameworkMethod method, final RunNotifier notifier) {
        final Description description = describeChild(method);
        if (this.isIgnored(method)) {
            notifier.fireTestIgnored(description);
        } else {
            EachTestNotifier eachNotifier = new EachTestNotifier(notifier, description);
            eachNotifier.fireTestStarted();
            boolean ignored = false;
            try {
                this.methodBlock(method).evaluate();
            } catch (AssumptionViolatedException ave) {
                eachNotifier.addFailedAssumption(ave);
            } catch (Throwable e) {
                if (validateForGraphComputer(e)) {
                    eachNotifier.fireTestIgnored();
                    logger.error(e.getMessage());
                    ignored = true;
                } else
                    eachNotifier.addFailure(e);
            } finally {
                if (!ignored)
                    eachNotifier.fireTestFinished();
            }
        }
    }

    private static boolean validateForGraphComputer(final Throwable e) {
        Throwable ex = e;
        while (ex != null) {
            if (ex instanceof VerificationException)
                return true;
            else if (ex instanceof NotSerializableException)
                return true;
            ex = ex.getCause();
        }
        return false;
    }
}
