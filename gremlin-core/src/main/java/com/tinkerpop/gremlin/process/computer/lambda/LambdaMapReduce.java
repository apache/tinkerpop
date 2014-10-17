package com.tinkerpop.gremlin.process.computer.lambda;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.util.AbstractVertexProgramBuilder;
import com.tinkerpop.gremlin.process.computer.util.LambdaHolder;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.util.function.TriConsumer;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.javatuples.Pair;

import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LambdaMapReduce<MK, MV, RK, RV, R> implements MapReduce<MK, MV, RK, RV, R> {

    public static final String MAP_LAMBDA = "gremlin.lambdaMapReduce.mapLambda";
    public static final String COMBINE_LAMBDA = "gremlin.lambdaMapReduce.combineLambda";
    public static final String REDUCE_LAMBDA = "gremlin.lambdaMapReduce.reduceLambda";
    public static final String MEMORY_LAMBDA = "gremlin.lambdaMapReduce.memoryLambda";
    public static final String MEMORY_KEY = "gremlin.lambdaMapReduce.memoryKey";

    private LambdaHolder<BiConsumer<Vertex, MapEmitter<MK, MV>>> mapLambdaHolder;
    private LambdaHolder<TriConsumer<MK, Iterator<MV>, ReduceEmitter<RK, RV>>> combineLambdaHolder;
    private LambdaHolder<TriConsumer<MK, Iterator<MV>, ReduceEmitter<RK, RV>>> reduceLambdaHolder;
    private LambdaHolder<Function<Iterator<Pair<RK, RV>>, R>> memoryLambdaHolder;
    private String sideEffectKey;

    private LambdaMapReduce() {

    }

    @Override
    public void loadState(final Configuration configuration) {
        this.mapLambdaHolder = LambdaHolder.loadState(configuration, MAP_LAMBDA);
        this.combineLambdaHolder = LambdaHolder.loadState(configuration, COMBINE_LAMBDA);
        this.reduceLambdaHolder = LambdaHolder.loadState(configuration, REDUCE_LAMBDA);
        this.memoryLambdaHolder = LambdaHolder.loadState(configuration, MEMORY_LAMBDA);
        this.sideEffectKey = configuration.getString(MEMORY_KEY, null);
    }

    @Override
    public void storeState(final Configuration configuration) {
        if (null != this.mapLambdaHolder)
            this.mapLambdaHolder.storeState(configuration);
        if (null != this.combineLambdaHolder)
            this.combineLambdaHolder.storeState(configuration);
        if (null != this.reduceLambdaHolder)
            this.reduceLambdaHolder.storeState(configuration);
        if (null != this.memoryLambdaHolder)
            this.memoryLambdaHolder.storeState(configuration);
        configuration.setProperty(MEMORY_KEY, this.sideEffectKey);
    }

    @Override
    public boolean doStage(final Stage stage) {
        if (stage.equals(Stage.MAP))
            return null != this.mapLambdaHolder;
        else if (stage.equals(Stage.COMBINE))
            return null != this.combineLambdaHolder;
        else
            return null != this.reduceLambdaHolder;
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<MK, MV> emitter) {
        this.mapLambdaHolder.get().accept(vertex, emitter);
    }

    @Override
    public void combine(final MK key, final Iterator<MV> values, final ReduceEmitter<RK, RV> emitter) {
       this.combineLambdaHolder.get().accept(key, values, emitter);
    }

    @Override
    public void reduce(final MK key, final Iterator<MV> values, final ReduceEmitter<RK, RV> emitter) {
        this.reduceLambdaHolder.get().accept(key, values, emitter);
    }

    @Override
    public R generateFinalResult(final Iterator<Pair<RK, RV>> keyValues) {
        return null == this.memoryLambdaHolder ? (R) keyValues : this.memoryLambdaHolder.get().apply(keyValues);
    }

    @Override
    public String getMemoryKey() {
        return this.sideEffectKey;
    }

    @Override
    public String toString() {
        return StringFactory.mapReduceString(this, this.sideEffectKey);
    }

    //////////////////

    public static <MK, MV, RK, RV, R> Builder<MK, MV, RK, RV, R> build() {
        return new Builder<>();
    }

    public static class Builder<MK, MV, RK, RV, R>  {

        private final Configuration configuration = new BaseConfiguration();

        public Builder<MK, MV, RK, RV, R> map(final BiConsumer<Vertex, MapReduce.MapEmitter<MK, MV>> mapLambda) {
            LambdaHolder.storeState(this.configuration, LambdaHolder.Type.OBJECT, MAP_LAMBDA, mapLambda);
            return this;
        }

        public Builder<MK, MV, RK, RV, R> map(final Class<? extends BiConsumer<Vertex, MapReduce.MapEmitter<MK, MV>>> mapClass) {
            LambdaHolder.storeState(this.configuration, LambdaHolder.Type.SCRIPT, MAP_LAMBDA, mapClass);
            return this;
        }

        public Builder<MK, MV, RK, RV, R> map(final String scriptEngine, final String mapScript) {
            LambdaHolder.storeState(this.configuration, LambdaHolder.Type.SCRIPT, MAP_LAMBDA, new String[]{scriptEngine, mapScript});
            return this;
        }

        public Builder<MK, MV, RK, RV, R> map(final String setupScript) {
            return map(AbstractVertexProgramBuilder.GREMLIN_GROOVY, setupScript);
        }

        ////////////

        public Builder<MK, MV, RK, RV, R> combine(TriConsumer<MK, Iterator<MV>, MapReduce.ReduceEmitter<RK, RV>> combineLambda) {
            LambdaHolder.storeState(this.configuration, LambdaHolder.Type.OBJECT, COMBINE_LAMBDA, combineLambda);
            return this;
        }

        public Builder<MK, MV, RK, RV, R> combine(final Class<? extends TriConsumer<MK, Iterator<MV>, MapReduce.ReduceEmitter<RK, RV>>> combineClass) {
            LambdaHolder.storeState(this.configuration, LambdaHolder.Type.CLASS, COMBINE_LAMBDA, combineClass);
            return this;
        }

        public Builder<MK, MV, RK, RV, R> combine(final String scriptEngine, final String combineScript) {
            LambdaHolder.storeState(this.configuration, LambdaHolder.Type.SCRIPT, COMBINE_LAMBDA, new String[]{scriptEngine, combineScript});
            return this;
        }

        public Builder<MK, MV, RK, RV, R> combine(final String setupScript) {
            return combine(AbstractVertexProgramBuilder.GREMLIN_GROOVY, setupScript);
        }

        ////////////

        public Builder<MK, MV, RK, RV, R> reduce(TriConsumer<MK, Iterator<MV>, MapReduce.ReduceEmitter<RK, RV>> reduceLambda) {
            LambdaHolder.storeState(this.configuration, LambdaHolder.Type.OBJECT, REDUCE_LAMBDA, reduceLambda);
            return this;
        }

        public Builder<MK, MV, RK, RV, R> reduce(Class<? extends TriConsumer<MK, Iterator<MV>, MapReduce.ReduceEmitter<RK, RV>>> reduceClass) {
            LambdaHolder.storeState(this.configuration, LambdaHolder.Type.CLASS, REDUCE_LAMBDA, reduceClass);
            return this;
        }

        public Builder<MK, MV, RK, RV, R> reduce(final String scriptEngine, final String reduceScript) {
            LambdaHolder.storeState(this.configuration, LambdaHolder.Type.SCRIPT, REDUCE_LAMBDA, new String[]{scriptEngine, reduceScript});
            return this;
        }

        public Builder<MK, MV, RK, RV, R> reduce(final String setupScript) {
            return reduce(AbstractVertexProgramBuilder.GREMLIN_GROOVY, setupScript);
        }

        ////////////

        public Builder<MK, MV, RK, RV, R> memory(Function<Iterator<Pair<RK, RV>>, R> memoryLambda) {
            LambdaHolder.storeState(this.configuration, LambdaHolder.Type.OBJECT, MEMORY_LAMBDA, memoryLambda);
            return this;
        }

        public Builder<MK, MV, RK, RV, R> memory(Class<? extends Function<Iterator<Pair<RK, RV>>, R>> memoryClass) {
            LambdaHolder.storeState(this.configuration, LambdaHolder.Type.CLASS, MEMORY_LAMBDA, memoryClass);
            return this;
        }

        public Builder<MK, MV, RK, RV, R> memory(final String scriptEngine, final String memoryScript) {
            LambdaHolder.storeState(this.configuration, LambdaHolder.Type.SCRIPT, MEMORY_LAMBDA, new String[]{scriptEngine, memoryScript});
            return this;
        }

        public Builder<MK, MV, RK, RV, R> memory(final String setupScript) {
            return memory(AbstractVertexProgramBuilder.GREMLIN_GROOVY, setupScript);
        }

        ////////////

        public Builder<MK, MV, RK, RV, R> memoryKey(final String memoryKey) {
            this.configuration.setProperty(LambdaMapReduce.MEMORY_KEY, memoryKey);
            return this;
        }

        public LambdaMapReduce<MK, MV, RK, RV, R> create() {
            LambdaMapReduce<MK, MV, RK, RV, R> lambdaMapReduce = new LambdaMapReduce<>();
            lambdaMapReduce.loadState(this.configuration);
            return lambdaMapReduce;
        }
    }
}
