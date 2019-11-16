package swim

import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.suspendCancellableCoroutine
import java.util.concurrent.TimeUnit
import kotlin.coroutines.experimental.Continuation

class CoroutineDispatcher<K, V> {
    val lock = Any()
    val map = mutableMapOf<K, MutableList<Continuation<V>>>()

    suspend fun offer(key: K, value: V): Boolean {
        val lst = mutableListOf<Continuation<V>>()
        synchronized(lock) {
            if (map.containsKey(key)) {
                lst.addAll(map[key]!!)
                map.remove(key)
            }
        }
        for (item in lst) {
            item.resume(value)
        }
        return lst.isNotEmpty()
    }

    suspend fun pickUp(key: K, timeout: Long, unit: TimeUnit = TimeUnit.MILLISECONDS): V? {
        return suspendCancellableCoroutine { cont ->
            cont.invokeOnCompletion(true) {
                synchronized(lock) {
                    val list = map.computeIfAbsent(key) {
                        mutableListOf()
                    }
                    list.remove(cont)
                    if (list.isEmpty()) {
                        map.remove(key)
                    }
                }
            }
            launch {
                delay(timeout, unit)
                if (!cont.isCompleted) {
                    synchronized(lock) {
                        val list = map[key]
                        if (list != null) {
                            list.remove(cont)
                            if (list.isEmpty()) {
                                map.remove(key)
                            }
                        }
                    }
                    cont.resume(null)
                }
            }
            synchronized(lock) {
                map.computeIfAbsent(key, {
                    mutableListOf()
                }).add(cont)
            }
        }
    }
}