package org.janelia.saalfeldlab.paintera.control.assignment

import gnu.trove.map.hash.TLongIntHashMap
import gnu.trove.map.hash.TLongLongHashMap
import gnu.trove.map.hash.TLongObjectHashMap
import gnu.trove.set.hash.TLongHashSet
import javafx.beans.InvalidationListener
import net.imglib2.util.StopWatch
import org.janelia.saalfeldlab.fx.ObservableWithListenersList
import org.janelia.saalfeldlab.paintera.control.assignment.action.AssignmentAction
import org.janelia.saalfeldlab.paintera.id.IdService
import org.slf4j.LoggerFactory
import org.zeromq.ZMQ
import java.io.Closeable
import java.lang.invoke.MethodHandles
import java.nio.charset.Charset
import kotlin.math.sqrt

class FragmentSegmentAssignmentPias(
        val piasAddress: String,
        val idService: IdService,
        val context: ZMQ.Context,
        lastFragmentLabel: TLongIntHashMap = TLongIntHashMap(),
        examples: TLongObjectHashMap<TLongIntHashMap> = TLongObjectHashMap()
        ): ObservableWithListenersList(), FragmentSegmentAssignmentState, Closeable {

    private val examples = TLongObjectHashMap<TLongIntHashMap>()
    private val lastFragmentLabel = TLongIntHashMap()
    private val positiveEamples = TLongLongHashMap()
    private val negativeExamples = TLongLongHashMap()

    private var lookup = Lookup()

    private inner class Lookup(assignments: TLongLongHashMap = TLongLongHashMap()) {

        val fragmentSegment = TLongLongHashMap()
        val segmentFragment = TLongObjectHashMap<TLongHashSet>()

        init {
            val counts = TLongLongHashMap()
            assignments.forEachValue{ counts.put(it, counts[it] + 1); true }
            val rootMapping = TLongLongHashMap()
            assignments.forEachEntry { k, v ->
                if (counts[v] > 1) {
                    if (!rootMapping.containsKey(v)) {
                        val root = idService.next()
                        rootMapping.put(v, root)
                        segmentFragment.put(root, TLongHashSet())
                    }
                    val root = rootMapping[v]
                    fragmentSegment.put(k, root)
                    segmentFragment[root].add(k)
                }
                true
            }
        }
    }

    init {
        this.lastFragmentLabel.putAll(lastFragmentLabel)
        this.examples.putAll(examples)
        examples.forEachEntry { e1, value -> value.forEachEntry { e2, v -> (if (v==1) positiveEamples else negativeExamples).put(e1, e2);true }; true }
    }

    override fun getFragments(segmentId: Long): TLongHashSet {
        return lookup.segmentFragment[segmentId]?: TLongHashSet(longArrayOf(segmentId))
    }

    override fun getSegment(fragmentId: Long): Long {
        return lookup.fragmentSegment[fragmentId].let { if (it == lookup.fragmentSegment.noEntryValue) fragmentId else it }
    }

    override fun persist() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun apply(action: AssignmentAction?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun apply(actions: MutableCollection<out AssignmentAction>?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    private fun updateLookup(assignments: TLongLongHashMap) {
        lookup = Lookup(assignments)
        stateChanged()
    }
    override fun close() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    fun pingServer(recvTimeout: Int = -1, sendTimeout: Int = -1): String? {
        val socket = context.socket(ZMQ.REQ)
        socket.sendTimeOut = sendTimeout
        socket.receiveTimeOut = recvTimeout
        val pingAddress = pingAddress()
        LOG.debug("Pinging at {}", pingAddress)
        socket.connect(pingAddress)
        socket.send("")
        return socket.recvStr(Charset.defaultCharset()).let { LOG.debug("Received pong from {}: `{}'", pingAddress, it); it }
    }

    private fun pingAddress() = "$piasAddress-ping"

    companion object {
        private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())
    }



}

fun main(argv: Array<String>) {
    val context = ZMQ.context(10)
    val assignment = FragmentSegmentAssignmentPias("ipc:///tmp/pias", IdService.dummy(), context = context)
    val times = (0..100).map {
        val sw = StopWatch.createAndStart()
        assignment.pingServer(recvTimeout = 50)
        sw.stop()
        sw.seconds()
    }
    val mean = times.sum() / times.size
    val std = sqrt(times.map { it - mean }.map { it * it }.sum() / times.size)
    println("Ping statistics ${mean}s ± ${std}s")

}