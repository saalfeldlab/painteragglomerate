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
        private val solutionSubscriptionAddress: String,
        private val currentSolutionUpdatePublishAddress: String,
        private val solutionSubscriptionTopic: String = "",
        private val currentSolutionUpdatePublishTopic: String =""
) : CurrentSolutionServer, Closeable {

    companion object {
        private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())
    }

    var currentSolution = TLongLongHashMap()
        private set

    private val serverSolutionSubscriber = ServerSolutionSubscriber(context, solutionSubscriptionAddress, solutionSubscriptionTopic, ::updateSolutionFromByteArray)

    val solutionRequestSocket: ZMQ.Socket
    val currentSolutionUpdatePublishSocket: ZMQ.Socket

    init {
        solutionRequestSocket = clientSocket(context, solutionRequestAddress)
        currentSolutionUpdatePublishSocket = publisherSocket(context, currentSolutionUpdatePublishAddress)
    }


    @Throws(CurrentSolutionServer.UnableToUpdate::class)
    override fun updateCurrentSolution() {
        val sentSuccessfully = solutionRequestSocket.send("")

        if (!sentSuccessfully)
            throw CurrentSolutionServer.UnableToUpdate()

        val response = solutionRequestSocket.recv()

        updateSolutionFromByteArray(response)

    }

    override fun currentSolution(): TLongLongHashMap {
        return TLongLongHashMap(currentSolution)
    }

    override fun close() {
        solutionRequestSocket.close()
        serverSolutionSubscriber.close()
    }

    @Synchronized
    private fun solutionChanged(solution: TLongLongHashMap)
    {
        LOG.debug("New solution: {}", solution)
        currentSolution = solution
        LOG.warn("Publishing envelope for topic `{}' at address `{}'", currentSolutionUpdatePublishTopic, currentSolutionUpdatePublishAddress)
        val envelopeWasSent = currentSolutionUpdatePublishSocket.sendMore(currentSolutionUpdatePublishTopic)
        LOG.warn("Publishing updated solution on topic `{}' at address `{}'? {}", currentSolutionUpdatePublishTopic, currentSolutionUpdatePublishAddress, envelopeWasSent)
        val contentsWereSent = currentSolutionUpdatePublishSocket.send(toBytesFromMap(solution), 0)
        LOG.warn("Published? {}", contentsWereSent)
    }

    @Throws(CurrentSolutionServer.UnableToUpdate::class)
    private fun updateSolutionFromByteArray(data: ByteArray)
    {
        if (!isArraySizeValid(data))
            throw CurrentSolutionServer.UnableToUpdate()

        val newSolution = toMapFromSolverServer(data)

        if (!newSolution.equals(currentSolution))
        {
            solutionChanged(newSolution)
        }
    }

    private class ServerSolutionSubscriber(
            context: ZMQ.Context,
            val address: String,
            val topic: String,
            val updateSolutionFromByteArray: (ByteArray) -> Unit) : Closeable{


        val socket = subscriberSocket(context, address, topic)

        val isClosed = AtomicBoolean(false)

        val solutionSubscriptionThread = Thread {
            while(!Thread.currentThread().isInterrupted) {
                LOG.debug("Waiting for publication on topic `{}'", topic)
                val topic = socket.recvStr(Charset.defaultCharset())
                LOG.debug("Received message for topic `{}'", topic)
                val contents = socket.recv()
                LOG.debug("Received contents `{}' for topic `{}'", contents, topic)
                updateSolutionFromByteArray(contents)
            }
        }

        init {
            solutionSubscriptionThread.start()
        }

        override fun close() {
            isClosed.set(true)
            solutionSubscriptionThread.interrupt()
            socket.close()
        }

    }


}