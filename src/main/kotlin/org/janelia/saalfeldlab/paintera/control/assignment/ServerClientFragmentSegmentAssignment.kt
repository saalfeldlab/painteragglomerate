package org.janelia.saalfeldlab.paintera.control.assignment

import gnu.trove.set.hash.TLongHashSet
import org.janelia.saalfeldlab.paintera.control.assignment.action.AssignmentAction

class ServerClientFragmentSegmentAssignment : FragmentSegmentAssignmentState() {

    override fun applyImpl(action: AssignmentAction?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getSegment(fragmentId: Long): Long {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getFragments(segmentId: Long): TLongHashSet {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

}