package org.janelia.saalfeldlab.paintera.solver

import com.google.gson.GsonBuilder
import com.google.gson.JsonArray
import com.google.gson.JsonObject
import org.janelia.saalfeldlab.paintera.control.assignment.action.AssignmentAction
import org.janelia.saalfeldlab.paintera.control.assignment.action.Detach
import org.janelia.saalfeldlab.paintera.control.assignment.action.Merge
import org.janelia.saalfeldlab.paintera.util.zmq.sockets.CloseableSocket
import org.janelia.saalfeldlab.paintera.util.zmq.sockets.clientSocket
import org.janelia.saalfeldlab.paintera.util.zmq.sockets.serverSocket
import org.junit.Assert
import org.junit.Test
import org.slf4j.LoggerFactory
import org.zeromq.ZMQ
import java.lang.invoke.MethodHandles
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.util.function.BiConsumer

class ActionsMiddleManZMQTest {
    companion object {
        private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

        private fun compareActions(a1: AssignmentAction, a2: AssignmentAction): Boolean
        {
            return when(a1.type) {
                AssignmentAction.Type.MERGE -> a2.type.equals(AssignmentAction.Type.MERGE) && compareMerges(a1 as Merge, a2 as Merge)
                AssignmentAction.Type.DETACH -> a2.type.equals(AssignmentAction.Type.DETACH) && compareDetaches(a1 as Detach, a2 as Detach)
                else -> false
            }
        }

        private fun compareMerges(m1: Merge, m2: Merge): Boolean
        {
            return m1.fromFragmentId == m2.fromFragmentId
                && m1.intoFragmentId == m2.intoFragmentId
                && m1.segmentId == m2.segmentId
        }

        private fun compareDetaches(d1: Detach, d2: Detach): Boolean
        {
            return d1.fragmentFrom == d2.fragmentFrom && d1.fragmentId == d2.fragmentId
        }
    }

    @Test
    fun test() {

        val gson = GsonBuilder()
                .registerTypeAdapter(AssignmentAction::class.java, AssignmentActionJsonAdapter())
                .create()

        val serverActions = mutableListOf<AssignmentAction>()
        val actionList = mutableListOf< AssignmentAction >(
                Merge(1,2,3),
                Detach(4, 5),
                Merge(6, 7, 8))


        val clientActionAddress = "ipc://client-actions"
        val serverActionAddress = "ipc://server-actions"
        val context = ZMQ.context(1)
        LOG.debug("Started context")
        val actionsMiddleMan = ActionsMiddleManZMQ(
                context,
                clientActionAddress,
                serverActionAddress,
                500,
                300,
                BiConsumer{t, cause -> LOG.warn("Exception in thread {}", t, cause)})
        val clientActionsSocket = clientSocket(context, clientActionAddress, -1)
        val serverActionsSocket = CloseableSocket(serverSocket(context, serverActionAddress, receiveTimeout = -1))
        val serverActionsThread = Thread{
            val endpoint = serverActionsSocket.socket.recvStr(Charset.defaultCharset())!! // ?: continue
            LOG.debug("Received endpoint {}", endpoint)
            Assert.assertEquals(ActionsMiddleManZMQ.SUBMIT_ACTIONS_ENDPOINT, endpoint)
            val jsonString = serverActionsSocket.socket.recvStr(Charset.defaultCharset())!!
            val jsonActions = gson.fromJson(jsonString, JsonObject::class.java).get(ActionsMiddleManZMQ.ACTIONS_KEY).asJsonArray
            serverActionsSocket.socket.send(ByteBuffer.allocate(java.lang.Integer.BYTES).putInt(jsonActions.size()).array(), 0)
            LOG.debug("Received actions: {}", jsonActions)
            jsonActions.forEach({serverActions.add(gson.fromJson(it, AssignmentAction::class.java))})
        }
        serverActionsThread.start()

        val jsonActions = JsonArray()
        actionList.forEach({jsonActions.add(gson.toJsonTree(it, AssignmentAction::class.java))})
        clientActionsSocket.send(gson.toJson(jsonActions).toByteArray(Charset.defaultCharset()), 0)

        Thread.sleep(1000)

        actionsMiddleMan.close()
        serverActionsSocket.close()
        serverActionsThread.join()

        Assert.assertEquals(actionList.size, serverActions.size)
        for (i in 0 until actionList.size)
        {
            val a1 = actionList[i]
            val a2 = serverActions[i]

            Assert.assertTrue("Action mismatch at index $i: $a1 -- $a2", compareActions(a1, a2))
        }

    }





}