package swim

import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.runBlocking
import java.util.concurrent.TimeUnit

fun runSimulation(n: Int,
                  joinPhase: Long = 5,
                  simulationPhase: Long = 300,
                  netFactory: () -> Network,
                  nodeFactory: (Network) -> Node,
                  agentFactory: (Node) -> Agent) {

    runBlocking {
        val node0 = nodeFactory(netFactory())
        val nodes = (2..n).map { nodeFactory(netFactory()) }

        nodes.forEach { it.join(node0.network.myself) }

        val allNodes = mutableListOf<Node>()
        allNodes.add(node0)
        allNodes.addAll(nodes)

        val agents = allNodes.map { agentFactory(it) }
        delay(joinPhase, TimeUnit.SECONDS)
        agents.forEach { it.start() }
        for (t in 1..simulationPhase) {
            println("${String.format("%4d", t)}:" +
                    agents.map { it.state() }.joinToString(""))
            delay(1, TimeUnit.SECONDS)
        }
        agents.forEach { it.stop() }
        allNodes.forEach { it.network.shutdown() }
    }

}

fun main(args: Array<String>) {
    val shared = NettyShared()
    runSimulation(100,
            netFactory = { NettyNet(shared, SwimIO()) },
            nodeFactory = { SwimNode(it) },
            agentFactory = ::RandomJoinLeaveFail)

    shared.close()
}