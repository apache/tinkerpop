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
package org.apache.tinkerpop.gremlin.structure.io;

import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONCompatibility;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoCompatibility;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Compatibilities {

    public static final Compatibilities GRYO_ONLY = Compatibilities.with(GryoCompatibility.class);

    public static final Compatibilities UNTYPED_GRAPHSON = Compatibilities.with(GraphSONCompatibility.class)
            .configuredAs(".*no-types|v1d0");

    private final Class<? extends Enum<? extends Compatibility>> compatibility;

    /**
     * Initialized to 4.0.0 which is non-existent, but obviously the high-end of whatever would be tested.
     */
    private String releaseVersionBefore = "4.0.0";

    /**
     * Initialized to 3.0.0 which is non-existent, but obviously the low-end of whatever would be tested.
     */
    private String releaseVersionAfter = "3.0.0";

    /**
     * Initialized to 0.0 which is non-existent, but obviously the low-end of whatever would be tested.
     */
    private String ioVersionBefore = "4.0";

    /**
     * Initialized to 3.0 which is non-existent, but obviously the high-end of whatever would be tested.
     */
    private String ioVersionAfter = "0.0";

    private String configuredAs = ".*";

    private List<Compatibilities> compatibilitiesToJoin = Collections.emptyList();

    private Compatibilities(final Class<? extends Enum<? extends Compatibility>> c) {
        this.compatibility = c;
    }

    public static Compatibilities with(final Class<? extends Enum<? extends Compatibility>> c) {
        return new Compatibilities(c);
    }

    /**
     * Finds {@link Compatibility} instances before (and not inclusive of) the specified version.
     */
    public Compatibilities beforeRelease(final String before) {
        this.releaseVersionBefore = before;
        return this;
    }

    /**
     * Finds {@link Compatibility} instances after (and not inclusive of) the specified version.
     */
    public Compatibilities afterRelease(final String after) {
        this.releaseVersionAfter = after;
        return this;
    }

    /**
     * Finds {@link Compatibility} instances between (and not inclusive of) the specified versions.
     */
    public Compatibilities betweenReleases(final String start, final String end) {
        return beforeRelease(end).afterRelease(start);
    }

    /**
     * Finds {@link Compatibility} instances before (and not inclusive of) the specified version.
     */
    public Compatibilities before(final String before) {
        this.ioVersionBefore = before;
        return this;
    }

    /**
     * Finds {@link Compatibility} instances after (and not inclusive of) the specified version.
     */
    public Compatibilities after(final String after) {
        this.ioVersionAfter = after;
        return this;
    }

    /**
     * Finds {@link Compatibility} instances between (and not inclusive of) the specified versions.
     */
    public Compatibilities between(final String start, final String end) {
        return before(end).after(start);
    }

    public Compatibilities configuredAs(final String regex) {
        this.configuredAs = regex;
        return this;
    }

    public Compatibilities join(final Compatibilities... compatibilities) {
        this.compatibilitiesToJoin = Arrays.asList(compatibilities);
        return this;
    }

    public List<Compatibility> match() {
        final Compatibility[] enumArray = (Compatibility[]) compatibility.getEnumConstants();
        final List<Compatibility> enums = Arrays.asList(enumArray);
        final Pattern pattern = Pattern.compile(configuredAs);

        final List<Compatibility> thisMatch = enums.stream()
                .filter(c -> beforeRelease(c, releaseVersionBefore))
                .filter(c -> afterRelease(c, releaseVersionAfter))
                .filter(c -> beforeIo(c, ioVersionBefore))
                .filter(c -> afterIo(c, ioVersionAfter))
                .filter(c -> pattern.matcher(c.getConfiguration()).matches())
                .collect(Collectors.toList());
        final List<Compatibility> matches = new ArrayList<>(thisMatch);
        compatibilitiesToJoin.forEach(c -> matches.addAll(c.match()));
        return matches;
    }

    public Compatibility[] matchToArray() {
        final List<Compatibility> list = match();
        final Compatibility [] compatibilities = new Compatibility[list.size()];
        list.toArray(compatibilities);
        return compatibilities;
    }

    private static boolean afterRelease(final Compatibility version, final String after) {
        return versionCompare(version.getReleaseVersion(), after) > 0;
    }

    private static boolean beforeRelease(final Compatibility version, final String before) {
        return versionCompare(version.getReleaseVersion(), before) < 0;
    }

    private static boolean afterIo(final Compatibility version, final String after) {
        return versionCompare(version.getVersion(), after) > 0;
    }

    private static boolean beforeIo(final Compatibility version, final String before) {
        return versionCompare(version.getVersion(), before) < 0;
    }

    /**
     * @return The result is a negative integer if v1 is less than v2.
     *         The result is a positive integer if v1 is greater than v2.
     *         The result is zero if the strings are equal.
     */
    private static int versionCompare(final String v1, final String v2) {
        final String[] vals1 = v1.split("\\.");
        final String[] vals2 = v2.split("\\.");
        int i = 0;

        // set index to first non-equal ordinal or length of shortest version string
        while (i < vals1.length && i < vals2.length && vals1[i].equals(vals2[i])) {
            i++;
        }

        // compare first non-equal ordinal number
        if (i < vals1.length && i < vals2.length) {
            int diff = Integer.valueOf(vals1[i]).compareTo(Integer.valueOf(vals2[i]));
            return Integer.signum(diff);
        }

        // the strings are equal or one string is a substring of the other
        // e.g. "1.2.3" = "1.2.3" or "1.2.3" < "1.2.3.4"
        return Integer.signum(vals1.length - vals2.length);
    }
}
