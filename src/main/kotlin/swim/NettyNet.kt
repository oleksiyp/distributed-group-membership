package swim

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInitializer
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.DatagramPacket
import io.netty.channel.socket.nio.NioDatagramChannel
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.runBlocking
import java.net.InetSocketAddress
import java.net.SocketException
import java.util.*
import java.util.concurrent.TimeUnit

typealias NettyChannel = io.netty.channel.Channel


class NettyShared {
    val udpBootstrap = Bootstrap()
            .group(NioEventLoopGroup())
            .channel(NioDatagramChannel::class.java)

    fun close() {
        udpBootstrap.config().group().shutdownGracefully()
    }
}

class NettyNet(val shared: NettyShared, override val io: IO) : Network {

    override val input = Channel<Envelope>()
    override val output = Channel<Envelope>()

    val channel = runBlocking { shared.startListening() }
    override val myself: Address = Address(channel.localAddress() as InetSocketAddress)

    private suspend fun NettyShared.startListening(): NettyChannel {
        udpBootstrap.handler(object : ChannelInitializer<NioDatagramChannel>() {
            override fun initChannel(ch: NioDatagramChannel) {
                ch.pipeline().addLast(object : SimpleChannelInboundHandler<DatagramPacket>() {
                    override fun channelRead0(ctx: ChannelHandlerContext, msg: DatagramPacket) {
                        runBlocking {
                            val buf = msg.content()
                            input.send(Envelope(
                                    Address(msg.sender()),
                                    Address(msg.recipient()),
                                    io.decode(buf)
                            ))
                        }
                    }

                    override fun channelInactive(ctx: ChannelHandlerContext?) {
                        input.close()
                        output.close()
                    }
                })
            }

        })
        val channel = brutePorts()
        launch {
            for (envelope in output) {
                val buf = channel.alloc().buffer(128)
                io.encode(envelope.message, buf)
                channel.writeAndFlush(DatagramPacket(
                        buf,
                        envelope.to.sockAddr
                )).awaitDone()
            }
        }
        return channel
    }

    private suspend fun NettyShared.brutePorts(): NettyChannel {
        val rnd = Random()
        while (true) {
            val port = rnd.nextInt(65536 - 1024) + 1024
            try {
                return udpBootstrap.bind(port).awaitDone()
            } catch (ex: SocketException) {
                continue
            }
        }
    }

    override suspend fun shutdown() {
        channel.close().awaitDone()
    }
}


