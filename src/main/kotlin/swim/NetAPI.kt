package swim

import io.netty.buffer.ByteBuf
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.SendChannel
import java.net.InetSocketAddress

data class Address(val sockAddr: InetSocketAddress)

data class Envelope(val from: Address,
                    val to: Address,
                    val message: Message) {
    fun routeBack(route: List<Address>, block: (List<Address>) -> Message) =
            if (route.isEmpty())
                Envelope(to, from, block(listOf()))
            else
                Envelope(to, route.first(), block(route.takeLast(route.size - 1)))

    fun routeForward(route: List<Address>, block: (Address, List<Address>) -> Message): Envelope =
            Envelope(to, route.first(), block(route.first(), route.takeLast(route.size - 1)))
}

abstract class Message(val time: Long = System.currentTimeMillis())

interface IO {
    fun encode(msg: Message, buf: ByteBuf)
    fun decode(buf: ByteBuf): Message
}

interface Network {
    val myself: Address
    val io: IO
    val input: ReceiveChannel<Envelope>
    val output: SendChannel<Envelope>
    suspend fun shutdown()
}

interface Node {
    val network: Network
    suspend fun join(address: Address)
    suspend fun leave()
    suspend fun fail()
    suspend fun recover()
    suspend fun group(): List<Address>
}

interface Agent {
    val node: Node

    suspend fun start()

    suspend fun stop()

    suspend fun state(): Char
}