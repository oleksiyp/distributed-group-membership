package swim

import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.produce
import kotlinx.coroutines.experimental.launch
import java.util.*
import java.util.Collections.synchronizedMap

data class Member(
        val address: Address,
        var clock: Int,
        var ts: Long
) {
    fun tick() {
        synchronized(this) {
            ts = System.currentTimeMillis()
            clock++
        }
    }
}


class SwimNode(override val network: Network,
               val pingTimeout: Long = 1000,
               val nIndirect: Int = 10,
               val nMemebers: Int = 10,
               val tFail: Int = 10000) : Node {
    var self = Member(network.myself, 0, System.currentTimeMillis())

    val members = synchronizedMap(mutableMapOf<Address, Member>())
    lateinit var shuffledMembers: ReceiveChannel<Member>

    val pongDispatcher = CoroutineDispatcher<Long, Pong>()

    init {
        members[self.address] = self

        launch {
            for (envelope in network.input) {
                receive(envelope)
            }
        }
        launch {
            shuffledMembers = membersShuffle()
            pingerProcess()
        }
    }

    private suspend fun receive(envelope: Envelope) {
        self.tick()

        val msg = envelope.message
        when (msg) {
            is Ping -> {
                updateTable(msg.memberTable)
                if (msg.forwardRoute.isEmpty())
                    network.output.send(
                            envelope.routeBack(msg.backwardRoute,
                                    { route -> Pong(msg.seq,
                                            route,
                                            memberTable = selectPingMembers()) }))
                else
                    network.output.send(
                            envelope.routeForward(msg.forwardRoute,
                                    { addr, route ->
                                        Ping(msg.seq,
                                                forwardRoute = route,
                                                backwardRoute = listOf(addr) + msg.backwardRoute,
                                                memberTable = msg.memberTable + self.toItem())
                                    }))
            }
            is Pong -> {
                updateTable(msg.memberTable)
                if (!msg.backwardRoute.isEmpty())
                    network.output.send(
                            envelope.routeBack(msg.backwardRoute,
                                    { route ->
                                        Pong(msg.seq,
                                                route,
                                                msg.memberTable + self.toItem())
                                    }))
                else
                    pongDispatcher.offer(msg.seq, msg)
            }
        }
    }

    fun updateTable(membershipUpdate: List<MemberTableItem>) {
        synchronized(members) {
            for (updateMember in membershipUpdate) {
                val member = members[updateMember.address]
                if (member == null) {
                    members[updateMember.address] = Member(
                            updateMember.address,
                            updateMember.clock,
                            System.currentTimeMillis())
                } else if (member.clock < updateMember.clock) {
                    member.clock = updateMember.clock
                    member.ts = System.currentTimeMillis()
                }
            }
        }
    }

    private suspend fun pingerProcess() {
        while (true) {
            for (member in shuffledMembers) {
                pingMember(member.address)
            }
        }
    }

    private suspend fun pingMember(address: Address): Boolean {
        val seq = random.nextLong()
        network.output.send(
                Envelope(network.myself, address,
                        Ping(seq, memberTable = selectPingMembers())))

        val ret = pongDispatcher.pickUp(seq, pingTimeout)
        if (ret == null) {
            val members = (1..nIndirect).map { shuffledMembers.receive() }
            return pingMemberIndirect(members, address)
        }
        return true
    }

    private fun selectPingMembers(): List<MemberTableItem> {
        var lst = synchronized(members) { members.values.toMutableList() }
        lst.shuffle(random)
        lst.remove(self)https://www.etoro.com/portfolio/btc
        lst = lst.take(nMemebers).toMutableList()
        lst.add(self)

        return lst.map { it.toItem() }
    }

    private suspend fun pingMemberIndirect(intermediates: List<Member>, address: Address): Boolean {
        val jobs = intermediates.map { intermediate ->
            val seq = random.nextLong()
            network.output.send(
                    Envelope(network.myself, intermediate.address,
                            Ping(seq, listOf(address))))
            produce {
                send(pongDispatcher.pickUp(seq, pingTimeout) != null)
            }
        }

        return jobs.map { it.receive() }.firstOrNull { it } ?: false
    }

    private suspend fun membersShuffle() = produce<Member> {
        while (true) {
            val lst = synchronized(members) { members.values.toMutableList() }
            lst.shuffle(random)
            for (item in lst) {
                send(item)
            }
        }
    }

    fun Member.isAlive(): Boolean = System.currentTimeMillis() - ts > tFail

    suspend override fun join(address: Address) {
        pingMember(address)
    }

    suspend override fun leave() {
    }

    suspend override fun fail() {
    }

    suspend override fun recover() {
    }

    suspend override fun group(): List<Address> = synchronized(members) {
        members.filterValues {
            it.isAlive()
        }.keys.toList()
    }


    companion object {
        val random = Random()
    }
}


private fun Member.toItem() = MemberTableItem(address, clock)
