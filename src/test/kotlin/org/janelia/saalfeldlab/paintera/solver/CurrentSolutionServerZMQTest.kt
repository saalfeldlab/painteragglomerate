package org.janelia.saalfeldlab.paintera.solver

import gnu.trove.map.hash.TLongLongHashMap
import org.janelia.saalfeldlab.paintera.util.zmq.sockets.publisherSocket
import org.janelia.saalfeldlab.paintera.util.zmq.sockets.subscriberSocket
import org.janelia.saalfeldlab.paintera.util.zmq.toBytesFromMap
import org.janelia.saalfeldlab.paintera.util.zmq.toMapFromSolverServer
import org.junit.Assert
import org.junit.Test
import org.slf4j.LoggerFactory
import org.zeromq.ZMQ
import java.lang.invoke.MethodHandles
import java.nio.charset.Charset
import java.util.concurrent.CountDownLatch

class CurrentSolutionServerZMQTest {
    companion object {

        private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())
    }

    @Test
    fun test()
    {

        val solutionRequestAddress: String = "ipc://SOLUTION_REQUEST"
        val solutionSubscriptionAddress: String = "ipc://SOLUTION_SUBSCRIPTION"
        val currentSolutionUpdatePublishAddress: String = "ipc://SOLUTION_PUBLISHER"
        val solutionSubscriptionTopic: String = "SOL_SUB"
        val currentSolutionUpdatePublishTopic: String = ""//""SOL_PUB"


        val context = ZMQ.context(1)
        LOG.debug("Started context")

        context.use {


            val solutionServerSocket = publisherSocket(context, solutionSubscriptionAddress)
            val solutionSubscriberSocket = subscriberSocket(context, currentSolutionUpdatePublishAddress, currentSolutionUpdatePublishTopic)
            val latch = CountDownLatch(1)
            val updatedLocalMap = TLongLongHashMap()
            Thread {
                LOG.warn("Waiting for publication on topic `{}' at address `{}'", currentSolutionUpdatePublishTopic, currentSolutionUpdatePublishAddress)
                val envelope = solutionSubscriberSocket.recvStr(Charset.defaultCharset())
                LOG.warn("Received envelope `{}'", envelope)
                updatedLocalMap.putAll(toMapFromSolverServer(solutionSubscriberSocket.recv()))
                latch.countDown()
            }.start()

            val server = CurrentSolutionServerZMQ(
                    it,
                    solutionRequestAddress,
                    solutionSubscriptionAddress,
                    currentSolutionUpdatePublishAddress,
                    solutionSubscriptionTopic,
                    currentSolutionUpdatePublishTopic
            )
            LOG.debug("Started server")


            server.use {
                val initialSolution = it.currentSolution()
                Assert.assertEquals(TLongLongHashMap(), initialSolution)
                LOG.debug("Initial solution {}", initialSolution)
                val updatedSolution = TLongLongHashMap(longArrayOf(1, 2), longArrayOf(3, 4))

                solutionServerSocket.sendMore(solutionSubscriptionTopic)
                solutionServerSocket.send(toBytesFromMap(updatedSolution), 0)

                latch.await()

                Thread.sleep(50)

                LOG.warn("Received updated map: {}", updatedLocalMap)


            }
        }

    }
}