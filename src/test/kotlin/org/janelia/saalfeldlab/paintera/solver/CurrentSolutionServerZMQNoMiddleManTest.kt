package org.janelia.saalfeldlab.paintera.solver

import gnu.trove.map.hash.TLongLongHashMap
import org.janelia.saalfeldlab.paintera.util.zmq.sockets.publisherSocket
import org.janelia.saalfeldlab.paintera.util.zmq.sockets.serverSocket
import org.janelia.saalfeldlab.paintera.util.zmq.toBytesFromMap
import org.junit.Assert
import org.junit.Test
import org.slf4j.LoggerFactory
import org.zeromq.ZMQ
import java.lang.invoke.MethodHandles
import java.nio.charset.Charset
import java.util.concurrent.CountDownLatch
import java.util.function.Consumer

class CurrentSolutionServerZMQNoMiddleManTest {
    companion object {

        private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())
    }

    @Test
    fun test() {

        val solutionRequestAddress = "ipc://SOLUTION_REQUEST"
        val solutionSubscriptionAddress = "ipc://SOLUTION_SUBSCRIPTION"
        val solutionSubscriptionTopic = "SOLUTION_FROM_SERVER"


        val context = ZMQ.context(1)
        LOG.debug("Started context")


        // receiveTimeout is necessary to be able to stop thread properly
        val solutionResponseSocket = serverSocket(context, solutionRequestAddress, receiveTimeout = 100)

        val solutionServerSocket = publisherSocket(context, solutionSubscriptionAddress)
        val latch = CountDownLatch(1)
        val updatedLocalMap = TLongLongHashMap(longArrayOf(5), longArrayOf(6))


        val solutionResponseThread = Thread {
            Thread.currentThread().name = "solution-response-thread"
            while (!Thread.currentThread().isInterrupted()) {
                LOG.debug("Waiting for solution request at address {}", solutionRequestAddress)
                val request = solutionResponseSocket.recvStr(Charset.defaultCharset())
                LOG.debug("Got request {}", request)
                if (request != null && request.equals(CurrentSolutionZMQ.CURRENT_SOLUTION_REQUEST_ENDPOINT))
                    solutionResponseSocket.send(toBytesFromMap(updatedLocalMap), 0)
            }
            LOG.debug("Finished iterating in solutionResponseThread")
        }
        solutionResponseThread.start()

        val server = CurrentSolutionZMQ(
                context,
                solutionRequestAddress,
                solutionSubscriptionAddress,
                solutionSubscriptionTopic,
                receiveTimeout = 100
        )
        server.addSolutionListeners(Consumer{
            updatedLocalMap.clear()
            updatedLocalMap.putAll(it)
            latch.countDown()
        })

        // WTF WHY IS THIS EVEN NECESSARY?
        // Minimum wait time seems to be 200ms
        // https://stackoverflow.com/questions/18850482/zeromq-jzmq-recvzerocopy-fails-to-get-any-message-while-recv-works
        // "As a surprise, both ZeroMQ as well as some of the commercial MQ products, recommends a pause after creating a
        // topic, or several topics, before you actually start sending messages through."
        Thread.sleep(200)


        val initialSolution = server.currentSolution()
        LOG.debug("Started server with initial solution {}", initialSolution)
        Assert.assertEquals(updatedLocalMap, initialSolution)
        LOG.debug("Initial solution {}", initialSolution)
        val updatedSolution = TLongLongHashMap(longArrayOf(1, 2), longArrayOf(3, 4))

        LOG.debug("Sending updated solution {}", updatedSolution)
        solutionServerSocket.sendMore(solutionSubscriptionTopic)
        solutionServerSocket.send(toBytesFromMap(updatedSolution), 0)

        LOG.debug("Waiting for receipt of updated solution")
        latch.await()

        LOG.debug("Received updated map: {}", updatedLocalMap)
        Assert.assertEquals(updatedSolution, updatedLocalMap)

        LOG.debug("Interrupting solution response thread")
        solutionResponseThread.interrupt()
        solutionResponseThread.join()
        solutionResponseSocket.close()
        LOG.debug("Joined solution response thread")

        server.close()
        LOG.debug("Closed server")

         // for some reason, context.close() hangs forever
         // context.close()
    }


}