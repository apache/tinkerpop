package com.tinkerpop.blueprints.computer;

import com.tinkerpop.blueprints.Graph;

/**
 * Each vertex in a vertex-centric graph computation can access itself and its neighbors' properties.
 * However, in many situations, a global backboard (or distributed cache) is desired.
 * The {@link GraphMemory} is a synchronizing data structure that allows arbitrary vertex communication.
 * Moreover, the {@link GraphMemory} maintains global information about the computation such as the iterations and
 * runtime.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface GraphMemory {

    public <R> R get(final String key);

    public void setIfAbsent(final String key, final Object value);

    public long increment(final String key, final long delta);

    public long decrement(final String key, final long delta);

    public boolean and(final String key, final boolean bool);

    public boolean or(final String key, final boolean bool);

    public int getIteration();

    public long getRuntime();

    public boolean isInitialIteration();

    public Graph getGraph();

    /*public void min(String key, double value);

    public void max(String key, double value);

    public void avg(String key, double value);

    public <V> void update(String key, Function<V, V> update, V defaultValue);*/

    // public Map<String, Object> getCurrentState();
}
