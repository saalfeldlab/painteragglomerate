package org.janelia.saalfeldlab.paintera.control.assignment

import gnu.trove.impl.Constants
import gnu.trove.map.hash.TLongLongHashMap
import gnu.trove.map.hash.TLongObjectHashMap
import gnu.trove.set.hash.TLongHashSet
import net.imglib2.type.label.Label
import org.janelia.saalfeldlab.fx.ObservableWithListenersList
import org.janelia.saalfeldlab.paintera.control.assignment.action.AssignmentAction
import org.janelia.saalfeldlab.paintera.control.assignment.action.Detach
import org.janelia.saalfeldlab.paintera.control.assignment.action.Merge
import org.janelia.saalfeldlab.util.NamedThreadFactory
import org.slf4j.LoggerFactory
import java.lang.invoke.MethodHandles
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.function.Consumer

class ServerClientFragmentSegmentAssignment(val broadcaster: AssignmentActionBroadcaster) :
        ObservableWithListenersList(), FragmentSegmentAssignmentState, Consumer<TLongLongHashMap> {

    override fun persist() {
        LOG.debug("Nothing to persist here.")
    }

    override fun accept(solution: TLongLongHashMap)
    {
        applySolution(solution)
    }

    // https://github.com/saalfeldlab/bigcat/tree/8daba4571b5f1f3b6616c0db625332cf18091a64

    // static stuff
    companion object {
        private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())
    }


    private val fragmentToSegmentMap = TLongLongHashMap(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, Label.TRANSPARENT, Label.TRANSPARENT)

    private val segmentToFragmentsMap = TLongObjectHashMap<TLongHashSet>(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, Label.TRANSPARENT)

    private val history = ArrayList<AssignmentAction>()

    private val submittedActions = HashSet<AssignmentAction>()

    private val receiveThread: ScheduledExecutorService = Executors.newScheduledThreadPool(1, NamedThreadFactory("${javaClass.simpleName}-receieve", true))

    init {

        stateChanged()
        receiveThread.scheduleWithFixedDelay({
            synchronized(this) {
                history.removeAll(submittedActions)
                submittedActions.clear()
                history.forEach(::applyOnly)
            }
        }, 0, 10, TimeUnit.MILLISECONDS)

    }

    private fun applySolution(solution: TLongLongHashMap): Boolean {
        LOG.warn("Got solution {}", solution)
        synchronized(this) {
            fragmentToSegmentMap.clear()
            fragmentToSegmentMap.putAll(solution)
            syncILut()
        }
        stateChanged()
        return true
    }

    private fun applyOnly(action: AssignmentAction) {
        LOG.debug("Applying action {}", action)
        when (action.type) {
            AssignmentAction.Type.MERGE -> {
                LOG.debug("Applying merge {}", action)
                mergeFragmentsImpl(action as Merge)
            }
            AssignmentAction.Type.DETACH -> {
                LOG.debug("Applying detach {}", action)
                detachFragmentImpl(action as Detach)
            }
            else -> {

            }
        }
    }

    override fun apply(action: AssignmentAction) {
        history.add(action)
        submittedActions.add(action)
        broadcaster.broadcast(action)
        applyOnly(action)
        stateChanged()
    }

    override fun apply(actions: Collection<AssignmentAction>) {
        history.addAll(actions)
        submittedActions.addAll(actions)
        broadcaster.broadcast(actions)
        actions.forEach(::applyOnly)
        stateChanged()
    }

    override fun getSegment(fragmentId: Long): Long {
        val id: Long
        val segmentId = fragmentToSegmentMap.get(fragmentId)
        id = if (segmentId == fragmentToSegmentMap.noEntryValue) {
            fragmentId
        } else {
            segmentId
        }
        LOG.debug("Returning {} for fragment {}: ", id, fragmentId)
        return id
    }

    override fun getFragments(segmentId: Long): TLongHashSet {
        val fragments = segmentToFragmentsMap.get(segmentId)
        return if (fragments == null) TLongHashSet(longArrayOf(segmentId)) else TLongHashSet(fragments)
    }

    @Synchronized
    private fun syncILut() {
        segmentToFragmentsMap.clear()
        val lutIterator = fragmentToSegmentMap.iterator()
        while (lutIterator.hasNext()) {
            lutIterator.advance()
            val fragmentId = lutIterator.key()
            val segmentId = lutIterator.value()
            var fragments: TLongHashSet? = segmentToFragmentsMap.get(segmentId)
            if (fragments == null) {
                fragments = TLongHashSet()
                fragments.add(segmentId)
                segmentToFragmentsMap.put(segmentId, fragments)
            }
            fragments.add(fragmentId)
        }
    }

    private fun mergeFragmentsImpl(merge: Merge) {

        LOG.debug("Merging {}", merge)

        val into = merge.intoFragmentId
        val from = merge.fromFragmentId
        val segmentInto = merge.segmentId

        LOG.trace("Current fragmentToSegmentMap {}", fragmentToSegmentMap)

        // If neither from nor into are assigned to a segment yet, both will
        // return fragmentToSegmentMap.getNoEntryKey() and we will falsely
        // return here
        // Therefore, check if from is contained. Alternatively, compare
        // getSegment( from ) == getSegment( to )
        if (fragmentToSegmentMap.contains(from) && fragmentToSegmentMap.get(from) == fragmentToSegmentMap.get(into)) {
            LOG.debug("Fragments already in same segment -- not merging")
            return
        }

        val segmentFrom = if (fragmentToSegmentMap.contains(from)) fragmentToSegmentMap.get(from) else from
        val fragmentsFrom = segmentToFragmentsMap.remove(segmentFrom)
        LOG.debug("From segment: {} To segment: {}", segmentFrom, segmentInto)

        if (!fragmentToSegmentMap.contains(into)) {
            LOG.debug("Adding segment {} to framgent {}", segmentInto, into)
            fragmentToSegmentMap.put(into, segmentInto)
        }

        if (!segmentToFragmentsMap.contains(segmentInto)) {
            val fragmentOnly = TLongHashSet()
            fragmentOnly.add(into)
            LOG.debug("Adding fragments {} for segmentInto {}", fragmentOnly, segmentInto)
            segmentToFragmentsMap.put(segmentInto, fragmentOnly)
        }
        LOG.debug("Framgents for from segment: {}", fragmentsFrom)

        if (fragmentsFrom != null) {
            val fragmentsInto = segmentToFragmentsMap.get(segmentInto)
            LOG.debug("Fragments into {}", fragmentsInto)
            fragmentsInto.addAll(fragmentsFrom)
            Arrays.stream(fragmentsFrom.toArray()).forEach { id -> fragmentToSegmentMap.put(id, segmentInto) }
        } else {
            segmentToFragmentsMap.get(segmentInto).add(from)
            fragmentToSegmentMap.put(from, segmentInto)
        }
    }

    private fun detachFragmentImpl(detach: Detach) {
        LOG.debug("Detach {}", detach)
        val segmentFrom = fragmentToSegmentMap.get(detach.fragmentId)
        if (fragmentToSegmentMap.get(detach.fragmentFrom) != segmentFrom) {
            LOG.debug("{} not in same segment -- return without detach", detach)
            return
        }

        val fragmentId = detach.fragmentId
        val fragmentFrom = detach.fragmentFrom

        this.fragmentToSegmentMap.remove(fragmentId)

        LOG.debug("Removing fragment={} from segment={}", fragmentId, segmentFrom)
        val fragments = this.segmentToFragmentsMap.get(segmentFrom)
        if (fragments != null) {
            fragments.remove(fragmentId)
            if (fragments.size() == 1) {
                this.fragmentToSegmentMap.remove(fragmentFrom)
                this.segmentToFragmentsMap.remove(segmentFrom)
            }
        }
    }


}