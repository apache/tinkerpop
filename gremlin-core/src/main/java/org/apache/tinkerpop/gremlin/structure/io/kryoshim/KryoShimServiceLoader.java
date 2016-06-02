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
package org.apache.tinkerpop.gremlin.structure.io.kryoshim;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.ServiceLoader;

public class KryoShimServiceLoader {

    private static volatile KryoShimService CACHED_SHIM_SERVICE;

    private static final Logger log = LoggerFactory.getLogger(KryoShimServiceLoader.class);

    /**
     * Set this system property to the fully-qualified name of a {@link KryoShimService}
     * package-and-classname to force it into service.  Setting this property causes the
     * priority-selection mechanism ({@link KryoShimService#getPriority()}) to be ignored.
     */
    public static final String SHIM_CLASS_SYSTEM_PROPERTY = "tinkerpop.kryo.shim";

    public static KryoShimService load(boolean forceReload) {

        if (null != CACHED_SHIM_SERVICE && !forceReload) {
            return CACHED_SHIM_SERVICE;
        }

        ArrayList<KryoShimService> services = new ArrayList<>();

        ServiceLoader<KryoShimService> sl = ServiceLoader.load(KryoShimService.class);

        KryoShimService result = null;

        synchronized (KryoShimServiceLoader.class) {
            if (forceReload) {
                sl.reload();
            }

            for (KryoShimService kss : sl) {
                services.add(kss);
            }
        }

        String shimClass = System.getProperty(SHIM_CLASS_SYSTEM_PROPERTY);

        if (null != shimClass) {
            for (KryoShimService kss : services) {
                if (kss.getClass().getCanonicalName().equals(shimClass)) {
                    log.info("Set {} provider to {} ({}) from system property {}={}",
                            KryoShimService.class.getSimpleName(), kss, kss.getClass(),
                            SHIM_CLASS_SYSTEM_PROPERTY, shimClass);
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
            }
        }


        if (null == result) {
            throw new IllegalStateException("Unable to load KryoShimService");
        }

        log.info("Set {} provider to {} ({}) because its priority value ({}) is the highest available",
                KryoShimService.class.getSimpleName(), result, result.getClass(), result.getPriority());

        return CACHED_SHIM_SERVICE = result;
    }

    public static KryoShimService load() {
        return load(false);
    }

    public static byte[] writeClassAndObjectToBytes(Object o) {
        KryoShimService shimService = load();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        shimService.writeClassAndObject(o, baos);

        return baos.toByteArray();
    }

    public static <T> T readClassAndObject(InputStream source) {
        KryoShimService shimService = load();

        return (T)shimService.readClassAndObject(source);
    }

    private enum KryoShimServiceComparator implements Comparator<KryoShimService> {
        INSTANCE;

        @Override
        public int compare(KryoShimService a, KryoShimService b) {
            int ap = a.getPriority();
            int bp = b.getPriority();

            if (ap < bp) {
                return -1;
            } else if (bp < ap) {
                return 1;
            } else {
                return a.getClass().getCanonicalName().compareTo(b.getClass().getCanonicalName());
            }
        }
    }
}
