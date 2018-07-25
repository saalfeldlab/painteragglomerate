package org.janelia.saalfeldlab.paintera.control.assignment

import org.janelia.saalfeldlab.paintera.control.assignment.action.AssignmentAction
import java.io.IOException

interface AssignmentActionBroadcaster {

    @Throws(IOException::class)
    fun broadcast(action: AssignmentAction)

    // TODO(USe reasonable default once available in Kotlin)
    @Throws(IOException::class)
//    @JvmDefault
    fun broadcast(actions: Collection<AssignmentAction>)
//    {
//        actions.forEach(::broadcast)
//    }

}