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
import java.util.stream.Stream;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ConfigurationEvaluator {

    private final List<Integer> minConnectionPoolSizeRange = Arrays.asList(1,4,8,12,16,32,64,96,128,160,192,224,256,384,512);
    private final List<Integer> maxConnectionPoolSizeRange = Arrays.asList(4,8,12,16,32,64,96,128,160,192,224,256,384,512);
    private final List<Integer> maxSimultaneousUsagePerConnectionRange = Arrays.asList(2,3,4,5,8,16,24,32,64,96,128);
    private final List<Integer> maxInProcessPerConnectionRange = Arrays.asList(1,2,4,8,12,16,32,64,96,128);
    private final List<Integer> workerPoolSizeRange = Arrays.asList(1,2,3,4,8,16,32);

    public Stream<String[]> generate(final String [] args) {
        final Set<Set<Integer>> configsTried = new HashSet<>();
        for (int iv = 0; iv < workerPoolSizeRange.size(); iv++) {
            for (int iw = 0; iw < maxInProcessPerConnectionRange.size(); iw++) {
                for (int ix = 0; ix < minConnectionPoolSizeRange.size(); ix++) {
                    for (int iy = 0; iy < maxConnectionPoolSizeRange.size(); iy++) {
                        for (int iz = 0; iz < maxSimultaneousUsagePerConnectionRange.size(); iz++) {
                            if (minConnectionPoolSizeRange.get(ix) <= maxConnectionPoolSizeRange.get(iy)) {
                                final Set s = new HashSet(Arrays.asList(iv, iw, ix, iy, iz));
                                if (!configsTried.contains(s)) {
                                    final Object[] argsToProfiler =
                                            Stream.of("workerPoolSize", workerPoolSizeRange.get(iv).toString(),
                                                      "maxInProcessPerConnection", maxInProcessPerConnectionRange.get(iw).toString(),
                                                      "minConnectionPoolSize", minConnectionPoolSizeRange.get(ix).toString(),
                                                      "maxConnectionPoolSize", maxConnectionPoolSizeRange.get(iy).toString(),
                                                      "maxSimultaneousUsagePerConnection", maxSimultaneousUsagePerConnectionRange.get(iz).toString(),
                                                      "noExit", Boolean.TRUE.toString()).toArray();

                                    final Object[] withExtraArgs = args.length > 0 ? Stream.concat(Stream.of(args), Stream.of(argsToProfiler)).toArray() : argsToProfiler;

                                    final String[] stringProfilerArgs = Arrays.copyOf(withExtraArgs, withExtraArgs.length, String[].class);
                                    System.out.println("Testing with: " + Arrays.toString(stringProfilerArgs));
                                    ProfilingApplication.main(stringProfilerArgs);

                                    configsTried.add(s);
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
