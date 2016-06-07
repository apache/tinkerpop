/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.spark.process.computer;

import org.apache.spark.serializer.KryoSerializer;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.spark.structure.Spark;
import org.apache.tinkerpop.gremlin.spark.structure.io.gryo.GryoRegistrator;
import org.apache.tinkerpop.gremlin.spark.structure.io.gryo.kryoshim.unshaded.UnshadedKryoShimService;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.KryoShimServiceLoader;

import java.util.Map;

import static org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.KryoShimServiceLoader.KRYO_SHIM_SERVICE;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SparkHadoopGraphGryoRegistratorProvider extends SparkHadoopGraphProvider {

    public Map<String, Object> getBaseConfiguration(final String graphName, final Class<?> test, final String testMethodName, final LoadGraphWith.GraphData loadGraphWith) {
        Spark.close();
        final Map<String, Object> config = super.getBaseConfiguration(graphName, test, testMethodName, loadGraphWith);
        // ensure the context doesn't stay open for the GryoSerializer tests to follow
        // this is primarily to ensure that the KryoShimService loaded specifically in these tests don't leak to the other tests
        config.put(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, false);
        config.put("spark.serializer", KryoSerializer.class.getCanonicalName());
        config.put("spark.kryo.registrator", GryoRegistrator.class.getCanonicalName());
        System.setProperty(KRYO_SHIM_SERVICE, UnshadedKryoShimService.class.getCanonicalName());
        KryoShimServiceLoader.load(true);
        System.clearProperty(KRYO_SHIM_SERVICE);
        return config;
    }
}
