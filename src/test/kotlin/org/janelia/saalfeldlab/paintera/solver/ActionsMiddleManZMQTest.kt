package org.janelia.saalfeldlab.paintera.solver

import com.google.gson.GsonBuilder
import org.janelia.saalfeldlab.paintera.control.assignment.action.AssignmentAction
import org.janelia.saalfeldlab.paintera.control.assignment.action.Merge
import org.janelia.saalfeldlab.paintera.util.zmq.sockets.clientSocket
import org.junit.Test
import org.slf4j.LoggerFactory
import org.zeromq.ZMQ
import java.lang.invoke.MethodHandles

class ActionsMiddleManZMQTest {
    companion object {

        private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())
    }

    @Test
    fun test() {

        val gson = GsonBuilder()
                .registerTypeAdapter(AssignmentAction::class.java, AssignmentActionJsonAdapter())
                .create()

        val clientActionAddress = "ipc://client-actions"
        val context = ZMQ.context(1)
        LOG.debug("Started context")
        val actionsMiddleMan = ActionsMiddleManZMQ(context, clientActionAddress, 500, 100)
        var clientActionsSocket = clientSocket(context, clientActionAddress, -1)

        val actionList = arrayOf(Merge(1,2,3))
        clientActionsSocket.send(gson.toJson(actionList).toString())

    }


}