package org.janelia.saalfeldlab.paintera.solver

import com.google.gson.GsonBuilder
import org.janelia.saalfeldlab.paintera.control.assignment.action.AssignmentAction
import org.janelia.saalfeldlab.paintera.util.zmq.sockets.CloseableSocket
import org.janelia.saalfeldlab.paintera.util.zmq.sockets.serverSocket
import org.slf4j.LoggerFactory
import org.zeromq.ZMQ
import java.io.Closeable
import java.lang.invoke.MethodHandles
import java.nio.charset.Charset
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean

class ActionsMiddleManZMQ(
        context: ZMQ.Context,
        clientActionsAddress: String,
        submitTimeout: Int,
        receiveTimeout: Int = -1
) : ActionsMiddleMan, Closeable {

    companion object {
        private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())
    }

    private val queue = ArrayList<AssignmentAction>()
    private val clientActionReceiver = ClientActionReceiver(context, clientActionsAddress, ::addActions, receiveTimeout = receiveTimeout)

    override fun close() {
        clientActionReceiver.close()
    }

    private fun addActions(actions: Collection<AssignmentAction>)
    {
        LOG.warn("Adding all actions {} to queue {}", actions, queue)
        synchronized(queue)
        {
            queue.addAll(actions)
        }
    }

    private class ClientActionReceiver(
            context: ZMQ.Context,
            address: String,
            actionsHandler: (Collection<AssignmentAction>) -> Unit,
            receiveTimeout: Int = -1
    ) : CloseableSocket(serverSocket(context, address, receiveTimeout = receiveTimeout)) {

        val isCommunicating = AtomicBoolean(false)
        val communicationLock = java.lang.Object()

        val clientRequestsThread = Thread {
            Thread.currentThread().name = "server-solution-subscriber-thread"
            // gson cannot run fromGson if not in same thread as where instantiated? Why?
            val gson = GsonBuilder()
                    .registerTypeAdapter(AssignmentAction::class.java, AssignmentActionJsonAdapter())
                    .create()
            while (!super.isClosed.get()) {
                synchronized(this)
                {
                    if (!super.isClosed.get()) {
                        LOG.warn("Waiting for request at address {}, timeout={}ms", address, receiveTimeout)
                        try {
                            isCommunicating.set(true)
                            val contents: String? = socket.recvStr(Charset.defaultCharset())
                            if (contents != null) {
                                LOG.warn("Received contents `{}' at address {}", contents, address)
                                try {
                                    LOG.warn("Deserializing as JsonArray: {}", contents)
                                    val jsonActions  = gson.toJsonTree(contents).asJsonArray
                                    LOG.warn("Contents as JsonArray: {}", jsonActions)
                                    val actions = ArrayList<AssignmentAction>()
                                    jsonActions.forEach({ actions.add(gson.fromJson(it, AssignmentAction::class.java)) })
                                    LOG.warn("Handling actions {}", actions)
                                    actionsHandler(actions)
                                    socket.send(byteArrayOf(1), 0)
                                } catch (e: java.lang.Exception) {
                                    e.printStackTrace()
                                    LOG.warn("Caught exception", e)
                                    socket.send(byteArrayOf(0), 0)
                                    // should we rethrow?
                                    throw e
                                }
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
                LOG.warn("End of iteration! is communicating? {} is closed? {}", isCommunicating.get(), isClosed.get())
            }
            LOG.warn("Done iterating! is communicating? {} is closed? {}", isCommunicating.get(), isClosed.get())
        }

        init {
            clientRequestsThread.start()
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
            clientRequestsThread.join()
            LOG.debug("Joined solutionSubscriptionThread")
        }

    }

}