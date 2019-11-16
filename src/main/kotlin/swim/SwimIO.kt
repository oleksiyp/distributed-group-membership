package swim

import io.netty.buffer.ByteBuf
import java.net.InetAddress
import java.net.InetSocketAddress

data class MemberTableItem(val address: Address, val clock: Int)

data class Ping(
        val seq: Long,
        val forwardRoute: List<Address> = listOf(),
        val backwardRoute: List<Address> = listOf(),
        val memberTable: List<MemberTableItem> = listOf()
        ) : Message()

data class Pong(
        val seq: Long,
        val backwardRoute: List<Address> = listOf(),
        val memberTable: List<MemberTableItem> = listOf()
) : Message()

class SwimIO : IO {
    companion object {
        val PING_TAG = 0
        val PONG_TAG = 1
    }

    override fun encode(msg: Message, buf: ByteBuf) {
        when(msg) {
            is Ping -> {
                buf.writeInt(PING_TAG)
                buf.writeLong(msg.seq)
                writeRoute(buf, msg.forwardRoute)
                writeRoute(buf, msg.backwardRoute)
                writeTable(buf, msg.memberTable)
            }
            is Pong -> {
                buf.writeInt(PONG_TAG)
                buf.writeLong(msg.seq)
                writeRoute(buf, msg.backwardRoute)
                writeTable(buf, msg.memberTable)
            }
            else -> throw RuntimeException("unknown message type " + msg::class)
        }
    }

    private fun writeTable(buf: ByteBuf, table: List<MemberTableItem>) {
        buf.writeInt(table.size)
        for (item in table) {
            writeAddress(buf, item.address.sockAddr)
            buf.writeInt(item.clock)
        }
    }

    private fun writeRoute(buf: ByteBuf, route: List<Address>) {
        buf.writeInt(route.size)
        for (address in route) {
            writeAddress(buf, address.sockAddr)
        }
    }

    private fun writeAddress(buf: ByteBuf, address: InetSocketAddress) {
        val addr = address.address.address
        buf.writeInt(addr.size)
        buf.writeBytes(addr)
        buf.writeInt(address.port)
    }

    override fun decode(buf: ByteBuf): Message {
        val tag = buf.readInt()
        return when(tag) {
            PING_TAG -> Ping(
                    buf.readLong(),
                    readRoute(buf),
                    readRoute(buf),
                    readTable(buf))

            PONG_TAG -> Pong(
                    buf.readLong(),
                    readRoute(buf),
                    readTable(buf))
            else -> throw RuntimeException("unknown tag " + tag)
        }
    }

    private fun readTable(buf: ByteBuf): List<MemberTableItem> {
        val n = buf.readInt()
        return (1..n).map { MemberTableItem(Address(readAddress(buf)), buf.readInt()) }
    }

    private fun readRoute(buf: ByteBuf): List<Address> {
        val n = buf.readInt()
        return (1..n).map { Address(readAddress(buf)) }
    }

    private fun readAddress(buf: ByteBuf): InetSocketAddress {
        val sz = buf.readInt()
        val bb = ByteArray(sz)
        buf.readBytes(bb)
        val port = buf.readInt()
        return InetSocketAddress(InetAddress.getByAddress(bb), port)
    }
}