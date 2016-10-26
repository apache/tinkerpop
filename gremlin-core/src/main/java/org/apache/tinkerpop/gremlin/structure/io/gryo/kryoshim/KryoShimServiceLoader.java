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
package org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim;

import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.ServiceLoader;

/**
 * Loads the highest-priority or user-selected {@link KryoShimService}.
 */
public class KryoShimServiceLoader {

    private static volatile KryoShimService cachedShimService;
    private static volatile Configuration configuration;

    private static final Logger log = LoggerFactory.getLogger(KryoShimServiceLoader.class);

    /**
     * Set this system property to the fully-qualified name of a {@link KryoShimService}
     * package-and-classname to force it into service.  Setting this property causes the
     * priority-selection mechanism ({@link KryoShimService#getPriority()}) to be ignored.
     */
    public static final String KRYO_SHIM_SERVICE = "gremlin.io.kryoShimService";

    public static void applyConfiguration(final Configuration configuration) {
        KryoShimServiceLoader.configuration = configuration;
        load(true);
    }

    /**
     * Return a reference to the shim service.  This method may return a cached shim service
     * unless {@code forceReload} is true.  Calls to this method need not be externally
     * synchonized.
     *
     * @param forceReload if false, this method may use its internal service cache; if true,
     *                    this method must ignore cache, and it must invoke {@link ServiceLoader#reload()}
     *                    before selecting a new service to return
     * @return the shim service
     */
    public static KryoShimService load(final boolean forceReload) {

        if (null != cachedShimService && !forceReload) {
            return cachedShimService;
        }

        final ArrayList<KryoShimService> services = new ArrayList<>();

        final ServiceLoader<KryoShimService> sl = ServiceLoader.load(KryoShimService.class);

        KryoShimService result = null;

        synchronized (KryoShimServiceLoader.class) {
            if (forceReload) {
                sl.reload();
            }

            for (KryoShimService kss : sl) {
                services.add(kss);
            }
        }

        String shimClass = null != configuration && configuration.containsKey(KRYO_SHIM_SERVICE) ?
                configuration.getString(KRYO_SHIM_SERVICE) :
                System.getProperty(KRYO_SHIM_SERVICE);

        if (null != shimClass) {
            for (KryoShimService kss : services) {
                if (kss.getClass().getCanonicalName().equals(shimClass)) {
                    log.info("Set {} provider to {} ({}) from system property {}={}",
                            KryoShimService.class.getSimpleName(), kss, kss.getClass(),
                            KRYO_SHIM_SERVICE, shimClass);
                    result = kss;
                }
            }
        } else {
            Collections.sort(services, KryoShimServiceComparator.INSTANCE);

            for (KryoShimService kss : services) {
                log.debug("Found Kryo shim service class {} (priority {})", kss.getClass(), kss.getPriority());
            }

            if (0 != services.size()) {
                result = services.get(services.size() - 1);

                log.info("Set {} provider to {} ({}) because its priority value ({}) is the best available",
                        KryoShimService.class.getSimpleName(), result, result.getClass(), result.getPriority());
            }
        }


        if (null == result) {
            throw new IllegalStateException("Unable to load KryoShimService");
        }

        final Configuration userConf = configuration;

        if (null != userConf) {
            log.info("Configuring {} provider {} with user-provided configuration",
                    KryoShimService.class.getSimpleName(), result);
            result.applyConfiguration(userConf);
        }

        return cachedShimService = result;
    }

    /**
     * Equivalent to {@link #load(boolean)} with the parameter {@code true}.
     *
     * @return the (possibly cached) shim service
     */
    public static KryoShimService load() {
        return load(false);
    }

    /**
     * A loose abstraction of {@link org.apache.tinkerpop.shaded.kryo.Kryo#writeClassAndObject},
     * where the {@code output} parameter is an internally-created {@link ByteArrayOutputStream}.  Returns
     * the byte array underlying that stream.
     *
     * @param o an object for which the instance and class are serialized
     * @return the serialized form
     */
    public static byte[] writeClassAndObjectToBytes(final Object o) {
        final KryoShimService shimService = load();

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        shimService.writeClassAndObject(o, baos);

        return baos.toByteArray();
    }

    /**
     * A loose abstraction of {@link org.apache.tinkerpop.shaded.kryo.Kryo#readClassAndObject},
     * where the {@code input} parameter is {@code source}.  Returns the deserialized object.
     *
     * @param source an input stream containing data for a serialized object class and instance
     * @param <T>    the type to which the deserialized object is cast as it is returned
     * @return the deserialized object
     */
    public static <T> T readClassAndObject(final InputStream source) {
        final KryoShimService shimService = load();

        return (T) shimService.readClassAndObject(source);
    }

    /**
     * Selects the service with greatest {@link KryoShimService#getPriority()}
     * (not absolute value).
     * <p>
     * Breaks ties with lexicographical comparison of classnames where the
     * name that sorts last is considered to have highest priority.  Ideally
     * nothing should rely on that tiebreaking behavior, but it beats random
     * selection in case a user ever gets into that situation by accident and
     * tries to figure out what's going on.
     */
    private enum KryoShimServiceComparator implements Comparator<KryoShimService> {
        INSTANCE;

        @Override
        public int compare(final KryoShimService a, final KryoShimService b) {
            final int ap = a.getPriority();
            final int bp = b.getPriority();

            if (ap < bp) {
                return -1;
            } else if (bp < ap) {
                return 1;
            } else {
                final int result = a.getClass().getCanonicalName().compareTo(b.getClass().getCanonicalName());

                if (0 == result) {
                    log.warn("Found two {} implementations with the same canonical classname: {}.  " +
                                    "This may indicate a problem with the classpath/classloader such as " +
                                    "duplicate or conflicting copies of the file " +
                                    "META-INF/services/org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.KryoShimService.",
                            a.getClass().getCanonicalName());
                } else {
                    final String winner = 0 < result ? a.getClass().getCanonicalName() : b.getClass().getCanonicalName();
                    log.warn("{} implementations {} and {} are tied with priority value {}.  " +
                                    "Preferring {} to the other because it has a lexicographically greater classname.  " +
                                    "Consider setting the system property \"{}\" instead of relying on priority tie-breaking.",
                            KryoShimService.class.getSimpleName(), a, b, ap, winner, KRYO_SHIM_SERVICE);
                }

                return result;
            }
        }
    }
}