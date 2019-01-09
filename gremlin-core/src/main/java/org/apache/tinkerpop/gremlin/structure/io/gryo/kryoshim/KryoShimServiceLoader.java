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

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.tinkerpop.gremlin.util.SystemUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.*;

/**
 * Loads the highest-priority or user-selected {@link KryoShimService}.
 */
public class KryoShimServiceLoader {

    private static volatile KryoShimService cachedShimService;
    private static volatile Configuration configuration;
    private static String maskedProperties = ".+\\.(password|keyStorePassword|trustStorePassword)|spark.authenticate.secret";

    private static final Logger log = LoggerFactory.getLogger(KryoShimServiceLoader.class);

    /**
     * Set this system property to the fully-qualified name of a {@link KryoShimService}
     * package-and-classname to force it into service.  Setting this property causes the
     * priority-selection mechanism ({@link KryoShimService#getPriority()}) to be ignored.
     */
    public static final String KRYO_SHIM_SERVICE = "gremlin.io.kryoShimService";

    public static void applyConfiguration(final Configuration configuration) {
        if (null == KryoShimServiceLoader.configuration ||
                null == KryoShimServiceLoader.cachedShimService ||
                !KryoShimServiceLoader.configuration.getKeys().hasNext()) {
            KryoShimServiceLoader.configuration = configuration;
            load(true);
        }
    }

    public static void close() {
        if (null != cachedShimService)
            cachedShimService.close();
        cachedShimService = null;
        configuration = null;
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
    private static KryoShimService load(final boolean forceReload) {
        // if the service is loaded and doesn't need reloading, simply return in
        if (null != cachedShimService && !forceReload)
            return cachedShimService;

        // if a service is already loaded, close it
        if (null != cachedShimService)
            cachedShimService.close();

        // if the configuration is null, try and load the configuration from System.properties
        if (null == configuration)
            configuration = SystemUtil.getSystemPropertiesConfiguration("tinkerpop", true);

        // get all of the shim services
        final ArrayList<KryoShimService> services = new ArrayList<>();
        final ServiceLoader<KryoShimService> serviceLoader = ServiceLoader.load(KryoShimService.class);
        synchronized (KryoShimServiceLoader.class) {
            if (forceReload) serviceLoader.reload();
            for (final KryoShimService kss : serviceLoader) {
                services.add(kss);
            }
        }
        // if a shim service class is specified in the configuration, use it -- else, priority-based
        if (configuration.containsKey(KRYO_SHIM_SERVICE)) {
            for (final KryoShimService kss : services) {
                if (kss.getClass().getCanonicalName().equals(configuration.getString(KRYO_SHIM_SERVICE))) {
                    log.info("Set KryoShimService to {} because of configuration {}={}",
                            kss.getClass().getSimpleName(),
                            KRYO_SHIM_SERVICE,
                            configuration.getString(KRYO_SHIM_SERVICE));
                    cachedShimService = kss;
                    break;
                }
            }
        } else {
            Collections.sort(services, KryoShimServiceComparator.INSTANCE);
            for (final KryoShimService kss : services) {
                log.debug("Found KryoShimService: {} (priority {})", kss.getClass().getCanonicalName(), kss.getPriority());
            }
            if (0 != services.size()) {
                cachedShimService = services.get(services.size() - 1);
                log.info("Set KryoShimService to {} because its priority value ({}) is the best available",
                        cachedShimService.getClass().getSimpleName(), cachedShimService.getPriority());
            }
        }

        // no shim service was available
        if (null == cachedShimService)
            throw new IllegalStateException("Unable to load KryoShimService");

        // once the shim service is defined, configure it
        log.info("Configuring KryoShimService {} with the following configuration:\n#######START########\n{}\n########END#########",
                cachedShimService.getClass().getCanonicalName(),
                ConfigurationUtils.toString(maskedConfiguration(configuration)));
        cachedShimService.applyConfiguration(configuration);
        return cachedShimService;
    }

    private static Configuration maskedConfiguration(Configuration configuration) {
        Configuration maskedConfiguration = new BaseConfiguration();
        Iterator keys = configuration.getKeys();
        while(keys.hasNext()) {
            String key = (String)keys.next();
            if (key.matches(maskedProperties))
                maskedConfiguration.setProperty(key, "******");
            else
                maskedConfiguration.setProperty(key, configuration.getProperty(key));
        }
        return maskedConfiguration;
    }

    /**
     * A loose abstraction of {@link org.apache.tinkerpop.shaded.kryo.Kryo#writeClassAndObject},
     * where the {@code output} parameter is an internally-created {@link ByteArrayOutputStream}.  Returns
     * the byte array underlying that stream.
     *
     * @param object an object for which the instance and class are serialized
     * @return the serialized form
     */
    public static byte[] writeClassAndObjectToBytes(final Object object) {
        final KryoShimService shimService = load(false);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        shimService.writeClassAndObject(object, baos);
        return baos.toByteArray();
    }

    /**
     * A loose abstraction of {@link org.apache.tinkerpop.shaded.kryo.Kryo#readClassAndObject},
     * where the {@code input} parameter is {@code source}.  Returns the deserialized object.
     *
     * @param inputStream an input stream containing data for a serialized object class and instance
     * @param <T>         the type to which the deserialized object is cast as it is returned
     * @return the deserialized object
     */
    public static <T> T readClassAndObject(final InputStream inputStream) {
        final KryoShimService shimService = load(false);
        return (T) shimService.readClassAndObject(inputStream);
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