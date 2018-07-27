package org.janelia.saalfeldlab.paintera.solver

import gnu.trove.map.hash.TLongLongHashMap
import org.janelia.saalfeldlab.paintera.util.zmq.isArraySizeValid
import org.janelia.saalfeldlab.paintera.util.zmq.sockets.clientSocket
import org.janelia.saalfeldlab.paintera.util.zmq.sockets.publisherSocket
import org.janelia.saalfeldlab.paintera.util.zmq.sockets.subscriberSocket
import org.janelia.saalfeldlab.paintera.util.zmq.toBytesFromMap
import org.janelia.saalfeldlab.paintera.util.zmq.toMapFromSolverServer
import org.slf4j.LoggerFactory
import org.zeromq.ZMQ
import java.io.Closeable
import java.lang.invoke.MethodHandles
import java.nio.charset.Charset
import java.util.concurrent.atomic.AtomicBoolean

class CurrentSolutionServerZMQ(
        context: ZMQ.Context,
        solutionRequestAddress: String,
        solutionSubscriptionAddress: String,
        currentSolutionUpdatePublishAddress: String,
        solutionSubscriptionTopic: String = "",
        currentSolutionUpdatePublishTopic: String = "",
        receiveTimeout: Int = -1
) : CurrentSolutionServer, Closeable {

    companion object {
        private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())
    }

    private var currentSolution = TLongLongHashMap()

    private val solutionPublisher = SolutionPublisher(context, currentSolutionUpdatePublishAddress, currentSolutionUpdatePublishTopic)
    private val serverSolutionSubscriber = ServerSolutionSubscriber(
            context,
            solutionSubscriptionAddress,
            solutionSubscriptionTopic,
            ::updateSolutionFromByteArray,
            receiveTimeout = receiveTimeout)
    private val solutionRequestSocket: ZMQ.Socket = clientSocket(context, solutionRequestAddress)

    init {
        LOG.debug("Requesting initial solution at address {}", solutionRequestAddress)
        solutionRequestSocket.send("")
        LOG.debug("Requested initial solution at address {}", solutionRequestAddress)
        val initialSolutionAsBytes = solutionRequestSocket.recv()
        LOG.debug("Updating initial solution from bytes {}", initialSolutionAsBytes)
        updateSolutionFromByteArray(initialSolutionAsBytes)
    }


    @Throws(CurrentSolutionServer.UnableToUpdate::class)
    override fun updateCurrentSolution() {
        val sentSuccessfully = solutionRequestSocket.send("")

        if (!sentSuccessfully)
            throw CurrentSolutionServer.UnableToUpdate()

        val response: ByteArray = solutionRequestSocket.recv() ?: throw CurrentSolutionServer.UnableToUpdate()

        updateSolutionFromByteArray(response)

    }

    override fun currentSolution(): TLongLongHashMap {
        return TLongLongHashMap(currentSolution)
    }

    override fun close() {
        solutionRequestSocket.close()
        serverSolutionSubscriber.close()
        solutionPublisher.close()
    }

    @Synchronized
    private fun solutionChanged(solution: TLongLongHashMap) {
        LOG.debug("New solution: {}", solution)
        currentSolution = solution
        val contentsSent = solutionPublisher.publishSolution(solution)
        LOG.debug("Published new solution? {}", contentsSent)
    }

    @Throws(CurrentSolutionServer.UnableToUpdate::class)
    private fun updateSolutionFromByteArray(data: ByteArray) {
        if (!isArraySizeValid(data))
            throw CurrentSolutionServer.UnableToUpdate()

        val newSolution = toMapFromSolverServer(data)

        if (newSolution != currentSolution) {
            solutionChanged(newSolution)
        }
    }

    private open class CloseableSocket(val socket: ZMQ.Socket) : Closeable {

        val isClosed = AtomicBoolean(false)

        override fun close() {
            isClosed.set(true)
            socket.close()
        }


    }

    private class SolutionPublisher(
            context: ZMQ.Context,
            private val address: String,
            private val topic: String) : CloseableSocket(publisherSocket(context, address)), Closeable {


        fun publishSolution(solution: TLongLongHashMap): Boolean {
            LOG.debug("Publishing envelope for topic `{}' at address `{}'", topic, address)
            val envelopeWasSent = socket.sendMore(topic)
            LOG.debug("Publishing updated solution {} on topic `{}' at address `{}'? {}", solution, topic, address, envelopeWasSent)
            val contentsWereSent = socket.send(toBytesFromMap(solution), 0)
            LOG.debug("Published? {}", contentsWereSent)
            return contentsWereSent
        }

    }

    private class ServerSolutionSubscriber(
            context: ZMQ.Context,
            private val address: String,
            private val topic: String,
            private val updateSolutionFromByteArray: (ByteArray) -> Unit,
            private val receiveTimeout: Int = -1) : CloseableSocket(subscriberSocket(context, address, topic, receiveTimeout = receiveTimeout)), Closeable {

        val isCommunicating = AtomicBoolean(false)
        val communicationLock = java.lang.Object()

        val solutionSubscriptionThread = Thread {
            Thread.currentThread().name = "server-solution-subscriber-thread"
            while (!super.isClosed.get()) {
                synchronized(this)
                {
                    if (!super.isClosed.get()) {
                        LOG.debug("Waiting for publication on topic `{}' at address {}, timeout={}ms", topic, address, receiveTimeout)
                        try {
                            isCommunicating.set(true)
                            val topic: String? = socket.recvStr(Charset.defaultCharset())
                            if (topic != null) {
                                LOG.debug("Received message for topic `{}' at address {}", topic, address)
                                val contents = socket.recv()
                                LOG.debug("Received contents `{}' for topic `{}' at address {}", contents, topic, address)
                                updateSolutionFromByteArray(contents)
                            }
                        } finally {
                            isCommunicating.set(false)
                            synchronized(communicationLock)
                            {
                                LOG.debug("Notifying all")
                                communicationLock.notifyAll()
                            }
                        }
                    }
                }
                // for some reason, need to sleep for a milli second here. Otherwise, loop keeps going.
                // Does synchronized block never exit in this loop otherwise?
                // if LOG.debug is replaced with LOG.warn, it works as well
                Thread.sleep(1)
                LOG.debug("End of iteration! is communicating? {} is closed? {}", isCommunicating.get(), isClosed.get())
            }
            LOG.debug("Done iterating! is communicating? {} is closed? {}", isCommunicating.get(), isClosed.get())
        }

        init {
            solutionSubscriptionThread.start()
        }

        override fun close() {
            LOG.debug("Calling {}.close()", javaClass.name)
            synchronized(this)
            {
                LOG.debug("Closing {}. Is communicating? {}", this.javaClass.name, isCommunicating.get())
                if (isCommunicating.get()) {
                    synchronized(communicationLock)
                    {
                        LOG.debug("Waiting for communcationLock. Is communicating? {}", isCommunicating)
                        communicationLock.wait()
                    }
                }
                LOG.debug("Calling super.close")
                super.close()
            }
            solutionSubscriptionThread.join()
            LOG.debug("Joined solutionSubscriptionThread")
        }

    }


}