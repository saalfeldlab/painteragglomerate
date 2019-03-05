package org.janelia.saalfeldlab.paintera.control.assignment

import gnu.trove.list.array.TByteArrayList
import gnu.trove.map.TLongIntMap
import gnu.trove.map.TLongObjectMap
import gnu.trove.map.hash.TLongIntHashMap
import gnu.trove.map.hash.TLongLongHashMap
import gnu.trove.map.hash.TLongObjectHashMap
import gnu.trove.set.hash.TLongHashSet
import javafx.beans.InvalidationListener
import net.imglib2.util.StopWatch
import org.janelia.saalfeldlab.fx.ObservableWithListenersList
import org.janelia.saalfeldlab.paintera.control.assignment.action.AssignmentAction
import org.janelia.saalfeldlab.paintera.id.IdService
import org.janelia.saalfeldlab.paintera.util.zmq.sockets.toBytes
import org.slf4j.LoggerFactory
import org.zeromq.ZMQ
import java.io.Closeable
import java.lang.Exception
import java.lang.invoke.MethodHandles
import java.nio.Buffer
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.util.Arrays
import kotlin.math.sqrt

private object codes {

    object solutionRequest {
        object responseCodes {
            val _SUCCESS = 0
            val _NO_SOLUTION_AVAILABLE = 1
        }
    }

    object setEdge {
        object responseCodes {
            val _SET_EDGE_REP_SUCCESS = 0
            val _SET_EDGE_REP_DO_NOT_UNDERSTAND = 1
            val _SET_EDGE_REP_EXCEPTION = 2
        }

        object requestCodes {
            val _SET_EDGE_REQ_EDGE_LIST = 0
        }
    }


}

data class Edge (val e1: Long, val e2: Long)

data class LabeledEdge(val edge: Edge, val label: Int) {
    constructor(e1: Long, e2: Long, label: Int): this(Edge(e1, e2), label)

    fun serializeToBuffer(buffer: ByteBuffer) {
        buffer.putLong(edge.e1)
        buffer.putLong(edge.e2)
        buffer.putInt(label)
    }
}

class FragmentSegmentAssignmentPias(
        val piasAddress: String,
        val idService: IdService,
        val context: ZMQ.Context,
        lastFragmentLabel: TLongIntHashMap = TLongIntHashMap(),
        examples: TLongObjectHashMap<TLongIntHashMap> = TLongObjectHashMap()
        ): ObservableWithListenersList(), FragmentSegmentAssignmentState {

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

    private fun bytesToLookup(bytes: ByteArray) {
        require(bytes.size % (2 * java.lang.Long.BYTES) == 0, {"Received byte array that is not integer multiple of 2 longs: ${bytes.size} -- ${Arrays.toString(bytes)}"})
        updateLookup(ByteBuffer.wrap(bytes).let {
            val map = TLongLongHashMap()
            while (it.hasRemaining()) {
                val k = it.long;
                val v = it.long;
                map.put(k, v)
            }
            map
        })

    }

    // TODO maybe throw exception is more informative than true/false
    fun updateSolution(recvTimeout: Int = -1, sendTimeout: Int = -1): Boolean {
        val socket = context.socket(ZMQ.REQ)
        socket.sendTimeOut = sendTimeout
        socket.receiveTimeOut = recvTimeout
        val solutionRequestAddress = requestCurrentSolutionAddress()
        socket.connect(solutionRequestAddress)
        LOG.info("Requesting current solution at {}", solutionRequestAddress)
        socket.send("")
        return socket.recv()?.let {
            val responseCode = ByteBuffer.wrap(it).int
            LOG.info(("Received response code $responseCode"))
            when(responseCode) {
                0 -> {bytesToLookup(socket.recv());true}
                1 -> false
                else -> false
            }
        } ?: false
    }

    fun setEdgeLabels(labels: Collection<LabeledEdge>, recvTimeout: Int = -1, sendTimeout: Int = -1) {

        val socket = createSocket(context, ZMQ.REQ, recvTimeout = recvTimeout, sendTimeout = sendTimeout)
        val address = setEdgeLabelsAddress()
        LOG.info("Connecting set edge labels socket to $address")
        socket.connect(address)
        socket.send(codes.setEdge.requestCodes._SET_EDGE_REQ_EDGE_LIST.toBytes(), ZMQ.SNDMORE)
        // 20 = 8 + 8 + 4 bytes; plus one int for method
        val bytes = ByteArray(20 * labels.size)
        ByteBuffer.wrap(bytes).let { labels.forEach { le -> le.serializeToBuffer(it) } }
        socket.send(bytes, 0)
        val responseCode = socket.recv()?.let { ByteBuffer.wrap(it).int } ?: throw Exception("Response code was null!")

        when(responseCode) {
            codes.setEdge.responseCodes._SET_EDGE_REP_DO_NOT_UNDERSTAND -> throw Exception("Server did not understand method ${socket.recv()?.let { ByteBuffer.wrap(it).int }}")
            codes.setEdge.responseCodes._SET_EDGE_REP_EXCEPTION -> throw Exception("Server threw exception when trying to add labeled edges: ${socket.recvStr(Charset.defaultCharset())}")
            codes.setEdge.responseCodes._SET_EDGE_REP_SUCCESS -> {}// TODO update edges in here
            else -> throw Exception("Do not understand response code $responseCode")
        }
    }

    fun setEdgeLabels(labels: TLongObjectMap<TLongIntMap>, recvTimeout: Int = -1, sendTimeout: Int = -1) {

        val socket = createSocket(context, ZMQ.REQ, recvTimeout = recvTimeout, sendTimeout = sendTimeout)
        val address = setEdgeLabelsAddress()
        LOG.info("Connecting set edge labels socket to $address")
        socket.connect(address)
        socket.send(codes.setEdge.requestCodes._SET_EDGE_REQ_EDGE_LIST.toBytes(), ZMQ.SNDMORE)
        // 20 = 8 + 8 + 4 bytes; plus one int for method
        val bytes = ByteArray(20 * labels.size())
        ByteBuffer.wrap(bytes).let { labels.forEachEntry { a, b -> b.forEachEntry { c, d -> it.putLong(a); it.putLong(c); it.putInt(d);true }; true } }
        socket.send(bytes, 0)
        val responseCode = socket.recv()?.let { ByteBuffer.wrap(it).int } ?: throw Exception("Response code was null!")

        when(responseCode) {
            codes.setEdge.responseCodes._SET_EDGE_REP_DO_NOT_UNDERSTAND -> throw Exception("Server did not understand method ${socket.recv()?.let { ByteBuffer.wrap(it).int }}")
            codes.setEdge.responseCodes._SET_EDGE_REP_EXCEPTION -> throw Exception("Server threw exception when trying to add labeled edges: ${socket.recvStr(Charset.defaultCharset())}")
            codes.setEdge.responseCodes._SET_EDGE_REP_SUCCESS -> {}// TODO update edges in here
            else -> throw Exception("Do not understand response code $responseCode")
        }


    }

    private fun pingAddress() = "$piasAddress-ping"
    private fun requestCurrentSolutionAddress() = "$piasAddress-current-solution"
    private fun setEdgeLabelsAddress() = "$piasAddress-set-edge-labels"

    companion object {
        private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

        private fun createSocket(context: ZMQ.Context, type: Int, recvTimeout: Int = -1, sendTimeout: Int = -1): ZMQ.Socket {
            val socket = context.socket(type)
            socket.receiveTimeOut = recvTimeout
            socket.sendTimeOut = sendTimeout
            return socket
        }
    }



}

fun main(argv: Array<String>) {
    val context = ZMQ.context(10)
    val assignment = FragmentSegmentAssignmentPias("ipc:///tmp/pias", IdService.dummy(), context = context)
    val times = (0..2).map {
        val sw = StopWatch.createAndStart()
        assignment.pingServer(recvTimeout = 50)!!
        sw.stop()
        sw.seconds()
    }
    println("Times: $times")
    val mean = times.sum() / times.size
    val std = sqrt(times.map { it - mean }.map { it * it }.sum() / times.size)
    println("Ping statistics ${mean}s ± ${std}s")
    println("Did get a new solution! ${assignment.updateSolution(recvTimeout = 50)}")
    val edges = arrayOf(LabeledEdge(1, 2, 0), LabeledEdge(2, 3, 1))
    assignment.setEdgeLabels(edges.toList())


}