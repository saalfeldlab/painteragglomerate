package org.janelia.saalfeldlab.paintera.solver

import com.google.gson.GsonBuilder
import com.google.gson.JsonArray
import com.google.gson.JsonObject
import org.janelia.saalfeldlab.paintera.control.assignment.action.AssignmentAction
import org.janelia.saalfeldlab.paintera.util.zmq.sockets.CloseableSocket
import org.janelia.saalfeldlab.paintera.util.zmq.sockets.clientSocket
import org.janelia.saalfeldlab.paintera.util.zmq.sockets.serverSocket
import org.janelia.saalfeldlab.util.NamedThreadFactory
import org.slf4j.LoggerFactory
import org.zeromq.ZMQ
import java.io.Closeable
import java.lang.invoke.MethodHandles
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.BiConsumer

class ActionsMiddleManZMQ(
        context: ZMQ.Context,
        clientActionsAddress: String,
        serverActionsAddress: String,
        submitTimeout: Long = 300,
        receiveTimeout: Int = -1,
        vararg notifyOnExceptionWhenSubmittingToServerListeners: BiConsumer<Thread, Throwable>
) : ActionsMiddleMan, Closeable {

    companion object {
        private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

        const val SUBMIT_ACTIONS_ENDPOINT = "/submit/actions"

        const val ACTIONS_KEY = "actions"
    }

    private val queue = ArrayList<AssignmentAction>()
    private val clientActionReceiver = ClientActionReceiver(context, clientActionsAddress, ::addActions, receiveTimeout = receiveTimeout)
    private val serverActionSubmitter = ServerActionSubmitter(
            context,
            serverActionsAddress,
            queue,
            submitTimeout = submitTimeout,
            receiveTimeout = receiveTimeout,
            notifyOnExceptionListeners = *notifyOnExceptionWhenSubmittingToServerListeners)

    fun addExceptionOnServerSubmissionListeners(vararg notifyOnExceptionWhenSubmittingToServerListeners: BiConsumer<Thread, Throwable>)
    {
        synchronized(serverActionSubmitter.notifyOnException)
        {
            serverActionSubmitter.notifyOnException.addAll(listOf(*notifyOnExceptionWhenSubmittingToServerListeners))
        }
    }

    override fun close() {
        clientActionReceiver.close()
        serverActionSubmitter.close()
    }

    private fun addActions(actions: Collection<AssignmentAction>) {
        LOG.debug("Adding all actions {} to queue {}", actions, queue)
        synchronized(queue)
        {
            queue.addAll(actions)
        }
    }

    private class UnableToSend : Exception()

    private class NoResponseFromServer : Exception()

    private class InconsistentResponseSize : Exception()

    private class ServerActionSubmitter(
            context: ZMQ.Context,
            address: String,
            queue: MutableCollection<AssignmentAction>,
            submitTimeout: Long = 300,
            receiveTimeout: Int = -1,
            vararg notifyOnExceptionListeners: BiConsumer<Thread, Throwable>
    ) : CloseableSocket(clientSocket(context, address, receiveTimeout = receiveTimeout)), Closeable {

        val scheduler = Executors.newScheduledThreadPool(1, NamedThreadFactory("server-action-submitter-scheduler-%d", false))!!
        val gson = GsonBuilder().registerTypeAdapter(AssignmentAction::class.java, AssignmentActionJsonAdapter()).create()!!
        val notifyOnException = mutableListOf(*notifyOnExceptionListeners)

        init
        {
            scheduler.scheduleWithFixedDelay({

                try {
                    val toSend = ArrayList<AssignmentAction>()
                    synchronized(queue) {
                        toSend.addAll(queue)
                    }

                    LOG.debug("Submitting actions for queue {}", toSend)


                    if (!toSend.isEmpty()) {
                        val jsonActions = JsonArray()
                        toSend.forEach({ jsonActions.add(gson.toJsonTree(it, AssignmentAction::class.java)) })
                        LOG.debug("Converted queue to json array {}", jsonActions)

                        val successfullySentEndpoint = super.socket.sendMore(SUBMIT_ACTIONS_ENDPOINT)
                        LOG.debug("Successfully sent endpoint {}? {}", SUBMIT_ACTIONS_ENDPOINT, successfullySentEndpoint)
                        if (!successfullySentEndpoint)
                            throw UnableToSend()

                        val jsonObj = JsonObject()
                        jsonObj.add(ACTIONS_KEY, jsonActions)
                        val jsonData = gson.toJson(jsonObj)
                        val successfullySent = super.socket.send(jsonData.toByteArray(Charset.defaultCharset()), 0)
                        LOG.debug("Successfully sent json data {}? {}", jsonData, successfullySent)
                        if (!successfullySent)
                            throw UnableToSend()

                        val response = super.socket.recv() ?: throw NoResponseFromServer()
                        LOG.debug("Received response from server: {}", response)
                        if (ByteBuffer.wrap(response).int != toSend.size) throw InconsistentResponseSize()

                        synchronized(queue)
                        {
                            queue.removeAll(toSend)
                            LOG.debug("Queue after removing submitted actions {}: {}", toSend, queue)
                        }

                    }
                } catch(e: Exception)
                {
                    LOG.debug("Notifying listeners about exception in Thread {}", Thread.currentThread(), e)
                    notifyListeners(Thread.currentThread(), e)
                }

            }, 0, submitTimeout, TimeUnit.MILLISECONDS)
        }

        override fun close() {
            scheduler.shutdown()
            super.close()
        }

        private fun notifyListeners(t: Thread, cause: Throwable)
        {
            synchronized(notifyOnException)
            {
                notifyOnException.forEach { it.accept(t, cause) }
            }
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
                        LOG.debug("Waiting for request at address {}, timeout={}ms", address, receiveTimeout)
                        try {
                            isCommunicating.set(true)
                            val contents: String? = socket.recvStr(Charset.defaultCharset())
                            if (contents != null) {
                                LOG.debug("Received contents `{}' at address {}", contents, address)
                                try {
                                    val jsonActions = gson.fromJson(contents, JsonArray::class.java) //gson.toJsonTree(contents).asJsonArray
                                    LOG.debug("Contents as JsonArray: {}", jsonActions)
                                    val actions = ArrayList<AssignmentAction>()
                                    jsonActions.forEach({ actions.add(gson.fromJson(it, AssignmentAction::class.java)) })
                                    LOG.debug("Handling actions {}", actions)
                                    actionsHandler(actions)
                                    socket.send(byteArrayOf(1), 0)
                                } catch (e: java.lang.Exception) {
                                    e.printStackTrace()
                                    LOG.debug("Caught exception", e)
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
                LOG.debug("End of iteration! is communicating? {} is closed? {}", isCommunicating.get(), isClosed.get())
            }
            LOG.debug("Done iterating! is communicating? {} is closed? {}", isCommunicating.get(), isClosed.get())
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