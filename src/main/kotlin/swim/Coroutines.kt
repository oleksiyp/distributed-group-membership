package swim

import io.netty.channel.ChannelFuture
import kotlinx.coroutines.experimental.suspendCancellableCoroutine

suspend fun ChannelFuture.awaitDone(): NettyChannel =
        suspendCancellableCoroutine { cont ->
            addListener { f ->
                if (f.isSuccess) {
                    cont.resume(channel())
                } else {
                    cont.resumeWithException(f.cause())
                }
            }
            if (isCancellable) {
                cont.invokeOnCompletion(true) {
                    if (!isCancelled) {
                        cancel(true)
                    }
                }
            }
        }

