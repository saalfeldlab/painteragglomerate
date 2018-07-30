package org.janelia.saalfeldlab.paintera.solver

import gnu.trove.map.hash.TLongLongHashMap
import org.janelia.saalfeldlab.paintera.util.zmq.isArraySizeValid
import org.janelia.saalfeldlab.paintera.util.zmq.sockets.CloseableSocket
import org.janelia.saalfeldlab.paintera.util.zmq.sockets.clientSocket
import org.janelia.saalfeldlab.paintera.util.zmq.sockets.subscriberSocket
import org.janelia.saalfeldlab.paintera.util.zmq.toMapFromSolverServer
import org.slf4j.LoggerFactory
import org.zeromq.ZMQ
import java.io.Closeable
import java.lang.invoke.MethodHandles
import java.nio.charset.Charset
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.Consumer

class CurrentSolutionZMQ(
        context: ZMQ.Context,
        solutionRequestAddress: String,
        solutionSubscriptionAddress: String,
        solutionSubscriptionTopic: String = "",
        receiveTimeout: Int = -1,
        vararg solutionUpdatedListeners: Consumer<TLongLongHashMap>
) : CurrentSolutionMiddleMan, Closeable {

    companion object {
        private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

        const val CURRENT_SOLUTION_REQUEST_ENDPOINT = "/request/solution"
    }

    private var currentSolution = TLongLongHashMap()

    private val serverSolutionSubscriber = ServerSolutionSubscriber(
            context,
            solutionSubscriptionAddress,
            solutionSubscriptionTopic,
            ::updateSolutionFromByteArray,
            receiveTimeout = receiveTimeout)
    private val solutionRequestSocket: ZMQ.Socket = clientSocket(context, solutionRequestAddress)

    private val solutionListeners = mutableListOf(*solutionUpdatedListeners)

    init {
        LOG.debug("Requesting initial solution at address {}", solutionRequestAddress)
        solutionRequestSocket.send(CURRENT_SOLUTION_REQUEST_ENDPOINT)
        LOG.debug("Requested initial solution at address {}", solutionRequestAddress)
        val initialSolutionAsBytes = solutionRequestSocket.recv()
        LOG.debug("Updating initial solution from bytes {}", initialSolutionAsBytes)
        updateSolutionFromByteArray(initialSolutionAsBytes)
    }

    public fun addSolutionListeners(vararg solutionUpdatedListeners: Consumer<TLongLongHashMap>)
    {
        solutionListeners.addAll(listOf(*solutionUpdatedListeners))
    }


    @Throws(CurrentSolutionMiddleMan.UnableToUpdate::class)
    override fun updateCurrentSolution() {
        val sentSuccessfully = solutionRequestSocket.send(CURRENT_SOLUTION_REQUEST_ENDPOINT)

        if (!sentSuccessfully)
            throw CurrentSolutionMiddleMan.UnableToUpdate()

        val response: ByteArray = solutionRequestSocket.recv() ?: throw CurrentSolutionMiddleMan.UnableToUpdate()

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
    private fun solutionChanged(solution: TLongLongHashMap) {
        LOG.debug("New solution: {}", solution)
        currentSolution = solution
        val solutionCopy = TLongLongHashMap(solution)
        solutionListeners.forEach { it.accept(solutionCopy) }
    }

    @Throws(CurrentSolutionMiddleMan.UnableToUpdate::class)
    private fun updateSolutionFromByteArray(data: ByteArray) {
        if (!isArraySizeValid(data))
            throw CurrentSolutionMiddleMan.UnableToUpdate()

        val newSolution = toMapFromSolverServer(data)

        if (newSolution != currentSolution) {
            solutionChanged(newSolution)
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