package org.janelia.saalfeldlab.paintera.control.assignment

import org.janelia.saalfeldlab.paintera.control.assignment.action.AssignmentAction
import org.janelia.saalfeldlab.paintera.solver.ActionsMiddleManZMQWithDirectActionSubmission
import org.zeromq.ZMQ
import java.io.Closeable
import java.util.function.BiConsumer

class ZMQAssignmentActionBroadcasterActionsHandler(
        context: ZMQ.Context,
        address: String,
        submitTimeout: Long = 300,
        receiveTimeout: Int = -1,
        vararg notifyOnExceptionWhenSubmittingToServerListeners: BiConsumer<Thread, Throwable>
) : AssignmentActionBroadcaster, Closeable {

    private val middleMan = ActionsMiddleManZMQWithDirectActionSubmission(
            context,
            address,
            submitTimeout = submitTimeout,
            receiveTimeout = receiveTimeout,
            notifyOnExceptionWhenSubmittingToServerListeners = *notifyOnExceptionWhenSubmittingToServerListeners)

    override fun broadcast(action: AssignmentAction) {
        broadcast(arrayListOf(action))
    }

    override fun broadcast(actions: Collection<AssignmentAction>) {
        middleMan.addActions(actions)
    }

    override fun close() {
        middleMan.close()
    }

}
