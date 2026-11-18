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
package examples.jpms.extensions.types;

import org.apache.tinkerpop.gremlin.structure.io.pdt.ProviderDefined;

import java.util.Objects;

/**
 * A composite provider-defined type. Its private fields and no-argument constructor are reached reflectively by
 * gremlin-core, which only works because the module opens this package to {@code org.apache.tinkerpop.gremlin.core}.
 */
@ProviderDefined(name = "examples:CurrencyAmount")
public final class CurrencyAmount {
    private String currency;
    private long minorUnits;

    private CurrencyAmount() {}

    public CurrencyAmount(final String currency, final long minorUnits) {
        this.currency = currency;
        this.minorUnits = minorUnits;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof CurrencyAmount)) return false;
        final CurrencyAmount that = (CurrencyAmount) o;
        return minorUnits == that.minorUnits && Objects.equals(currency, that.currency);
    }

    @Override
    public int hashCode() {
        return Objects.hash(currency, minorUnits);
    }

    @Override
    public String toString() {
        return currency + " " + minorUnits;
    }
}
