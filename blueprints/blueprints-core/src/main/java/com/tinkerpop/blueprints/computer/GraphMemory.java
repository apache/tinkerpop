package com.tinkerpop.blueprints.computer;

/**
 * Each vertex in a vertex-centric graph computation can access itself and its neighbors' properties.
 * However, in many situations, a global backboard (or distributed cache) is desired.
 * The GraphMemory is a synchronizing data structure that allows arbitrary vertex communication.
 * Moreover, the GraphMemory maintains global information about the computation such as the iterations and runtime.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface GraphMemory {

    public <R> R get(final String key);

    public void setIfAbsent(String key, Object value);

    public long increment(String key, long delta);

    public long decrement(String key, long delta);

    public int getIteration();

    public long getRuntime();

    public boolean isInitialIteration();

    /*public void min(String key, double value);

    public void max(String key, double value);

    public void avg(String key, double value);

    public <V> void update(String key, Function<V, V> update, V defaultValue);*/

    // public Map<String, Object> getCurrentState();
}
