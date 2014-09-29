package com.tinkerpop.gremlin.process.computer.lambda;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.util.LambdaType;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.TriConsumer;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.javatuples.Pair;

import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LambdaMapReduce<MK, MV, RK, RV, R> implements MapReduce<MK, MV, RK, RV, R> {

    public static final String LAMBDA_MAP_REDUCE_MEMORY_KEY = "gremlin.lambdaMapReduce.memoryKey";
    public static final String LAMBDA_MAP_REDUCE_MAP_LAMBDA = "gremlin.lambdaMapReduce.mapLambda";
    public static final String LAMBDA_MAP_REDUCE_COMBINE_LAMBDA = "gremlin.lambdaMapReduce.combineLambda";
    public static final String LAMBDA_MAP_REDUCE_REDUCE_LAMBDA = "gremlin.lambdaMapReduce.reduceLambda";
    public static final String LAMBDA_MAP_REDUCE_MEMORY_LAMBDA = "gremlin.lambdaMapReduce.memoryLambda";

    private static final String SUPPLIER_TYPE_KEY = "gremlin.lambdaMapReduce.supplierType";

    private LambdaType lambdaType;

    private Pair<?, BiConsumer<Vertex, MapEmitter<MK, MV>>> supplierMapLambda;
    private BiConsumer<Vertex, MapEmitter<MK, MV>> mapLambda;

    private Pair<?, TriConsumer<MK, Iterator<MV>, ReduceEmitter<RK, RV>>> supplierCombineLambda;
    private TriConsumer<MK, Iterator<MV>, ReduceEmitter<RK, RV>> combineLambda;

    private Pair<?, TriConsumer<MK, Iterator<MV>, ReduceEmitter<RK, RV>>> supplierReduceLambda;
    private TriConsumer<MK, Iterator<MV>, ReduceEmitter<RK, RV>> reduceLambda;

    private Pair<?, Function<Iterator<Pair<RK, RV>>, R>> supplierMemoryLambda;
    private Function<Iterator<Pair<RK, RV>>, R> memoryLambda;

    private String sideEffectKey;

    private LambdaMapReduce() {

    }

    @Override
    public void loadState(final Configuration configuration) {
        this.lambdaType = LambdaType.getType(configuration, SUPPLIER_TYPE_KEY);
        try {
            if (configuration.containsKey(LAMBDA_MAP_REDUCE_MAP_LAMBDA)) {
                this.supplierMapLambda = this.lambdaType.<BiConsumer<Vertex, MapEmitter<MK, MV>>>get(configuration, LAMBDA_MAP_REDUCE_MAP_LAMBDA);
                this.mapLambda = this.supplierMapLambda.getValue1();
            }
            if (configuration.containsKey(LAMBDA_MAP_REDUCE_COMBINE_LAMBDA)) {
                this.supplierCombineLambda = this.lambdaType.<TriConsumer<MK, Iterator<MV>, ReduceEmitter<RK, RV>>>get(configuration, LAMBDA_MAP_REDUCE_COMBINE_LAMBDA);
                this.combineLambda = this.supplierCombineLambda.getValue1();
            }
            if (configuration.containsKey(LAMBDA_MAP_REDUCE_REDUCE_LAMBDA)) {
                this.supplierReduceLambda = this.lambdaType.<TriConsumer<MK, Iterator<MV>, ReduceEmitter<RK, RV>>>get(configuration, LAMBDA_MAP_REDUCE_REDUCE_LAMBDA);
                this.reduceLambda = this.supplierReduceLambda.getValue1();
            }
            if (configuration.containsKey(LAMBDA_MAP_REDUCE_MEMORY_LAMBDA)) {
                this.supplierMemoryLambda = this.lambdaType.<Function<Iterator<Pair<RK, RV>>, R>>get(configuration, LAMBDA_MAP_REDUCE_MEMORY_LAMBDA);
                this.memoryLambda = this.supplierMemoryLambda.getValue1();
            } else {
                this.memoryLambda = s -> (R) s;
            }
            this.sideEffectKey = configuration.getString(LAMBDA_MAP_REDUCE_MEMORY_KEY, null);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public void storeState(final Configuration configuration) {
        this.lambdaType.set(configuration, SUPPLIER_TYPE_KEY, LAMBDA_MAP_REDUCE_MAP_LAMBDA, this.supplierMapLambda.getValue0());
        this.lambdaType.set(configuration, SUPPLIER_TYPE_KEY, LAMBDA_MAP_REDUCE_COMBINE_LAMBDA, this.supplierCombineLambda.getValue0());
        this.lambdaType.set(configuration, SUPPLIER_TYPE_KEY, LAMBDA_MAP_REDUCE_REDUCE_LAMBDA, this.supplierReduceLambda.getValue0());
        this.lambdaType.set(configuration, SUPPLIER_TYPE_KEY, LAMBDA_MAP_REDUCE_MEMORY_LAMBDA, this.supplierMemoryLambda.getValue0());
        configuration.setProperty(LAMBDA_MAP_REDUCE_MEMORY_KEY, this.sideEffectKey);
    }

    @Override
    public boolean doStage(final Stage stage) {
        if (stage.equals(Stage.MAP))
            return null != this.mapLambda;
        else if (stage.equals(Stage.COMBINE))
            return null != this.combineLambda;
        else
            return null != this.reduceLambda;
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<MK, MV> emitter) {
        this.mapLambda.accept(vertex, emitter);
    }

    @Override
    public void combine(final MK key, final Iterator<MV> values, final ReduceEmitter<RK, RV> emitter) {
        this.combineLambda.accept(key, values, emitter);
    }

    @Override
    public void reduce(final MK key, final Iterator<MV> values, final ReduceEmitter<RK, RV> emitter) {
        this.reduceLambda.accept(key, values, emitter);
    }

    @Override
    public R generateSideEffect(final Iterator<Pair<RK, RV>> keyValues) {
        return this.memoryLambda.apply(keyValues);
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    //////////////////

    public static <MK, MV, RK, RV, R> Builder<MK, MV, RK, RV, R> build() {
        return new Builder<>();
    }

    public static class Builder<MK, MV, RK, RV, R> {

        private Configuration configuration = new BaseConfiguration();

        public Builder<MK, MV, RK, RV, R> map(final BiConsumer<Vertex, MapReduce.MapEmitter<MK, MV>> mapLambda) {
            LambdaType.OBJECT.set(this.configuration, SUPPLIER_TYPE_KEY, LAMBDA_MAP_REDUCE_MAP_LAMBDA, mapLambda);
            return this;
        }

        public Builder<MK, MV, RK, RV, R> map(final String scriptEngine, final String mapScript) {
            LambdaType.SCRIPT.set(this.configuration, SUPPLIER_TYPE_KEY, LAMBDA_MAP_REDUCE_MAP_LAMBDA, new String[]{scriptEngine, mapScript});
            return this;
        }

        public Builder<MK, MV, RK, RV, R> combine(TriConsumer<MK, Iterator<MV>, MapReduce.ReduceEmitter<RK, RV>> combineLambda) {
            LambdaType.OBJECT.set(this.configuration, SUPPLIER_TYPE_KEY, LAMBDA_MAP_REDUCE_COMBINE_LAMBDA, combineLambda);
            return this;
        }

        public Builder<MK, MV, RK, RV, R> combine(final String scriptEngine, final String combineScript) {
            LambdaType.SCRIPT.set(this.configuration, SUPPLIER_TYPE_KEY, LAMBDA_MAP_REDUCE_COMBINE_LAMBDA, new String[]{scriptEngine, combineScript});
            return this;
        }

        public Builder<MK, MV, RK, RV, R> reduce(TriConsumer<MK, Iterator<MV>, MapReduce.ReduceEmitter<RK, RV>> reduceLambda) {
            LambdaType.OBJECT.set(this.configuration, SUPPLIER_TYPE_KEY, LAMBDA_MAP_REDUCE_REDUCE_LAMBDA, reduceLambda);
            return this;
        }

        public Builder<MK, MV, RK, RV, R> reduce(final String scriptEngine, final String reduceScript) {
            LambdaType.SCRIPT.set(this.configuration, SUPPLIER_TYPE_KEY, LAMBDA_MAP_REDUCE_REDUCE_LAMBDA, new String[]{scriptEngine, reduceScript});
            return this;
        }

        public Builder<MK, MV, RK, RV, R> memory(Function<Iterator<Pair<RK, RV>>, R> memoryLambda) {
            LambdaType.OBJECT.set(this.configuration, SUPPLIER_TYPE_KEY, LAMBDA_MAP_REDUCE_MEMORY_LAMBDA, memoryLambda);
            return this;
        }

        public Builder<MK, MV, RK, RV, R> memory(final String scriptEngine, final String memoryScript) {
            LambdaType.SCRIPT.set(this.configuration, SUPPLIER_TYPE_KEY, LAMBDA_MAP_REDUCE_MEMORY_LAMBDA, new String[]{scriptEngine, memoryScript});
            return this;
        }

        public Builder<MK, MV, RK, RV, R> memoryKey(final String memoryKey) {
            this.configuration.setProperty(LambdaMapReduce.LAMBDA_MAP_REDUCE_MEMORY_KEY, memoryKey);
            return this;
        }

        public LambdaMapReduce<MK, MV, RK, RV, R> create() {
            LambdaMapReduce<MK, MV, RK, RV, R> lambdaMapReduce = new LambdaMapReduce<>();
            lambdaMapReduce.loadState(this.configuration);
            return lambdaMapReduce;
        }
    }
}
