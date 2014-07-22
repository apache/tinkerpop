
/**
 * @author Daniel Kuppitz (http://thinkaurelius.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */

clock = { int loops = 100, Closure closure ->
    closure.call() // warmup
    (1..loops).collect {
        t = System.nanoTime()
        closure.call()
        ((System.nanoTime() - t) * 0.000001)
    }.mean()
}