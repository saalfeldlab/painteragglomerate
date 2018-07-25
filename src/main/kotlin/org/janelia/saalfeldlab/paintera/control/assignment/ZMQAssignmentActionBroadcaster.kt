package org.janelia.saalfeldlab.paintera.control.assignment

import com.google.gson.GsonBuilder
import com.google.gson.JsonArray
import org.janelia.saalfeldlab.paintera.control.assignment.action.AssignmentAction
import org.janelia.saalfeldlab.paintera.solver.AssignmentActionJsonAdapter
import org.zeromq.ZMQ
import java.io.Closeable

class ZMQAssignmentActionBroadcaster(
        context: ZMQ.Context,
        address: String
) : AssignmentActionBroadcaster, Closeable {

    val socket = createPublishSocket(context, address)

    val gson = GsonBuilder().registerTypeAdapter(AssignmentAction::class.java, AssignmentActionJsonAdapter()).create()

    override fun broadcast(action: AssignmentAction) {
        broadcast(arrayListOf(action))
    }

    override fun broadcast(actions: Collection<AssignmentAction>) {
        val arr = JsonArray()
        actions.forEach({ arr.add(gson.toJsonTree(it, AssignmentAction::class.java)) })
        socket.send(arr.toString())
    }

    override fun close() {
        socket.close()
    }

    companion object {
        private fun createPublishSocket(context: ZMQ.Context, address: String): ZMQ.Socket {
            val socket = context.socket(ZMQ.PUB)
            socket.bind(address)
            return socket
        }
    }

}
