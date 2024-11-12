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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.DatetimeHelper;

import java.math.BigInteger;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.Set;

/**
 * Reference implementation for date parsing step.
 *
 * @author Valentyn Kahamlyk
 */
public final class AsDateStep<S> extends ScalarMapStep<S, OffsetDateTime> {

    public AsDateStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected OffsetDateTime map(final Traverser.Admin<S> traverser) {
        final Object object = traverser.get();
        if (object == null)
            throw new IllegalArgumentException("Can't parse null as DateTime.");
        if (object instanceof OffsetDateTime)
            return (OffsetDateTime) object;
        if (object instanceof Byte || object instanceof Short || object instanceof Integer || object instanceof Long)
            // numbers handled as milliseconds since January 1, 1970, 00:00:00 GMT.
            return OffsetDateTime.ofInstant(Instant.ofEpochMilli(((Number) object).longValue()), ZoneOffset.UTC);
        if (object instanceof BigInteger) {
            try {
                return OffsetDateTime.ofInstant(Instant.ofEpochMilli(((BigInteger) object).longValueExact()), ZoneOffset.UTC);
            } catch (ArithmeticException ae) {
                throw new IllegalArgumentException("Can't parse " + object + " as Date.");
            }
        }

        if (object instanceof String) {
            try {
                return DatetimeHelper.parse((String) object);
            }
            catch (DateTimeParseException e) {
                throw new IllegalArgumentException("Can't parse " + object + " as OffsetDateTime.");
            }
        }

        throw new IllegalArgumentException("Can't parse " + object.getClass().getName() + " as OffsetDateTime.");
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this);
    }
}
