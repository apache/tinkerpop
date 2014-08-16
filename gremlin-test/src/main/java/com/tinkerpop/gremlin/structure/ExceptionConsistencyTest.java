package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.computer.ComputerResult;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.process.computer.SideEffects;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.computer.lambda.LambdaVertexProgram;
import com.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankVertexProgram;
import org.apache.commons.configuration.Configuration;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static com.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures.FEATURE_COMPUTER;
import static com.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS;
import static com.tinkerpop.gremlin.structure.Graph.Features.PropertyFeatures.FEATURE_PROPERTIES;
import static com.tinkerpop.gremlin.structure.Graph.Features.VariableFeatures.FEATURE_VARIABLES;
import static com.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS;
import static org.junit.Assert.*;

/**
 * Ensure that exception handling is consistent within Blueprints. It may be necessary to throw exceptions in an
 * appropriate order in order to ensure that these tests pass.  Note that some exception consistency checks are
 * in the {@link FeatureSupportTest}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Enclosed.class)
@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
public class ExceptionConsistencyTest {



    private static class MockVertexProgramBuilder implements VertexProgram.Builder {
        @Override
        public <P extends VertexProgram> P create() {
            return (P) PageRankVertexProgram.build().create();
        }

        @Override
        public VertexProgram.Builder configure(final Object... keyValues) {
            return this;
        }
    }

    /**
     * Mock {@link com.tinkerpop.gremlin.process.computer.VertexProgram} that just dummies up a way to set a property on a {@link com.tinkerpop.gremlin.structure.Vertex}.
     */
    private static class MockVertexProgramForVertex implements VertexProgram {
        private final String key;
        private final String val;
        private final Map<String, KeyType> computeKeys = new HashMap<>();

        public MockVertexProgramForVertex(final String key, final String val) {
            this.key = key;
            this.val = val;
        }

        @Override
        public void loadState(final Configuration configuration) {
        }

        @Override
        public void storeState(final Configuration configuration) {
        }

        @Override
        public void setup(final SideEffects sideEffects) {
        }

        @Override
        public void execute(final Vertex vertex, final Messenger messenger, final SideEffects sideEffects) {
            vertex.property(this.key, this.val);
        }

        @Override
        public boolean terminate(SideEffects sideEffects) {
            return true;
        }

        @Override
        public Set<String> getSideEffectComputeKeys() {
            return Collections.emptySet();
        }

        @Override
        public Map<String, KeyType> getElementComputeKeys() {
            return this.computeKeys;
        }
    }

    /**
     * Mock {@link com.tinkerpop.gremlin.process.computer.VertexProgram} that just dummies up a way to set a property on an {@link com.tinkerpop.gremlin.structure.Edge}.
     */
    private static class MockVertexProgramForEdge implements VertexProgram {
        private final String key;
        private final String val;
        private final Map<String, KeyType> computeKeys = new HashMap<>();

        public MockVertexProgramForEdge(final String key, final String val) {
            this.key = key;
            this.val = val;
        }

        @Override
        public void storeState(final Configuration configuration) {
        }

        @Override
        public void loadState(final Configuration configuration) {
        }

        @Override
        public void setup(final SideEffects sideEffects) {
        }

        @Override
        public void execute(final Vertex vertex, final Messenger messenger, final SideEffects sideEffects) {
            vertex.bothE().forEach(e -> e.<String>property(this.key, this.val));
        }

        @Override
        public boolean terminate(SideEffects sideEffects) {
            return true;
        }

        @Override
        public Set<String> getSideEffectComputeKeys() {
            return Collections.emptySet();
        }

        @Override
        public Map<String, KeyType> getElementComputeKeys() {
            return this.computeKeys;
        }
    }
}
