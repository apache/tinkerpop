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
package org.apache.tinkerpop.gremlin.driver.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * An internal application used to test out ranges of configuration parameters for Gremlin Driver.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ConfigurationEvaluator {

    private final List<Integer> minConnectionPoolSizeRange = Arrays.asList(4,8,12,16,32,64,96,128,192,256,384,512);
    private final List<Integer> maxConnectionPoolSizeRange = Arrays.asList(16,32,64,96,128,192,256,384,512);
    private final List<Integer> minSimultaneousUsagePerConnectionRange = Arrays.asList(4,8,16,24,32,64,96,128);
    private final List<Integer> maxSimultaneousUsagePerConnectionRange = Arrays.asList(4,8,16,24,32,64,96,128);
    private final List<Integer> minInProcessPerConnectionRange = Arrays.asList(2,4,8,16,32,64,96,128);
    private final List<Integer> maxInProcessPerConnectionRange = Arrays.asList(16,32,64,96,128);
    private final List<Integer> workerPoolSizeRange = Arrays.asList(1,2,3,4,8,16,32);
    private final List<Integer> nioPoolSizeRange = Arrays.asList(1,2,4);
    private final List<Integer> parallelismSizeRange = Arrays.asList(1,2,4,8,16);

    public Stream<String[]> generate(final String [] args) throws Exception {
        final Set<String> configsTried = new HashSet<>();

        // get ready for the some serious brute-force action here
        for (int ir = 0; ir < nioPoolSizeRange.size(); ir++) {
            for (int is = 0; is < parallelismSizeRange.size(); is++) {
                for (int it = 0; it < workerPoolSizeRange.size(); it++) {
                    for (int iu = 0; iu < minInProcessPerConnectionRange.size(); iu++) {
                        for (int iv = 0; iv < maxInProcessPerConnectionRange.size(); iv++) {
                            for (int iw = 0; iw < minConnectionPoolSizeRange.size(); iw++) {
                                for (int ix = 0; ix < maxConnectionPoolSizeRange.size(); ix++) {
                                    for (int iy = 0; iy < minSimultaneousUsagePerConnectionRange.size(); iy++) {
                                        for (int iz = 0; iz < maxSimultaneousUsagePerConnectionRange.size(); iz++) {
                                            if (minConnectionPoolSizeRange.get(iw) <= maxConnectionPoolSizeRange.get(ix)
                                                    && minInProcessPerConnectionRange.get(iu) <= maxInProcessPerConnectionRange.get(iv)
                                                    && minSimultaneousUsagePerConnectionRange.get(iy) <= maxSimultaneousUsagePerConnectionRange.get(iz)
                                                    && maxSimultaneousUsagePerConnectionRange.get(iz) <= maxInProcessPerConnectionRange.get(iv)) {
                                                final String s = String.join(",", String.valueOf(ir), String.valueOf(is), String.valueOf(it), String.valueOf(iu), String.valueOf(iv), String.valueOf(iw), String.valueOf(ix), String.valueOf(iy), String.valueOf(iz));
                                                if (!configsTried.contains(s)) {
                                                    final Object[] argsToProfiler =
                                                            Stream.of("nioPoolSize", nioPoolSizeRange.get(ir).toString(),
                                                                    "parallelism", parallelismSizeRange.get(is).toString(),
                                                                    "workerPoolSize", workerPoolSizeRange.get(it).toString(),
                                                                    "minInProcessPerConnection", minInProcessPerConnectionRange.get(iu).toString(),
                                                                    "maxInProcessPerConnection", maxInProcessPerConnectionRange.get(iv).toString(),
                                                                    "minConnectionPoolSize", minConnectionPoolSizeRange.get(iw).toString(),
                                                                    "maxConnectionPoolSize", maxConnectionPoolSizeRange.get(ix).toString(),
                                                                    "minSimultaneousUsagePerConnection", minSimultaneousUsagePerConnectionRange.get(iy).toString(),
                                                                    "maxSimultaneousUsagePerConnection", maxSimultaneousUsagePerConnectionRange.get(iz).toString(),
                                                                    "noExit", Boolean.TRUE.toString()).toArray();

                                                    final Object[] withExtraArgs = args.length > 0 ? Stream.concat(Stream.of(args), Stream.of(argsToProfiler)).toArray() : argsToProfiler;

                                                    final String[] stringProfilerArgs = Arrays.copyOf(withExtraArgs, withExtraArgs.length, String[].class);
                                                    System.out.println("Testing with: " + Arrays.toString(stringProfilerArgs));
                                                    ProfilingApplication.main(stringProfilerArgs);
                                                    TimeUnit.SECONDS.sleep(5);
                                                    configsTried.add(s);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        System.out.println(configsTried.size());
        return null;
    }

    public static void main(final String [] args) {
        try {
            new ConfigurationEvaluator().generate(args);
            System.exit(0);
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }
    }
}
