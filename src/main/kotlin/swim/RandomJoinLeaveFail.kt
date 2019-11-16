package swim

import kotlinx.coroutines.experimental.Job
import kotlinx.coroutines.experimental.launch
import java.util.concurrent.atomic.AtomicReference

class RandomJoinLeaveFail(override val node: Node) : Agent {
    var job = AtomicReference<Job>()

    suspend override fun start() {
        job.set(launch {

        })
    }

    suspend override fun stop() {
        node.leave()
        job.getAndSet(null).cancel()
    }

    suspend override fun state(): Char {
        val job = job.get()
        return if (job != null) {
            return (node.group().size % 10).toString()[0]
        } else {
            'S'
        }
    }
}