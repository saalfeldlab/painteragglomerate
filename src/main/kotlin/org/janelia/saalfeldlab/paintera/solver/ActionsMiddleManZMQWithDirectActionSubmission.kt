package org.janelia.saalfeldlab.paintera.solver

import com.google.gson.GsonBuilder
import com.google.gson.JsonArray
import com.google.gson.JsonObject
import org.janelia.saalfeldlab.paintera.control.assignment.action.AssignmentAction
import org.janelia.saalfeldlab.paintera.util.zmq.sockets.CloseableSocket
import org.janelia.saalfeldlab.paintera.util.zmq.sockets.clientSocket
import org.janelia.saalfeldlab.util.NamedThreadFactory
import org.slf4j.LoggerFactory
import org.zeromq.ZMQ
import java.io.Closeable
import java.lang.invoke.MethodHandles
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.function.BiConsumer

class ActionsMiddleManZMQWithDirectActionSubmission(
        context: ZMQ.Context,
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
        serverActionSubmitter.close()
    }

    fun addActions(actions: Collection<AssignmentAction>) {
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

}