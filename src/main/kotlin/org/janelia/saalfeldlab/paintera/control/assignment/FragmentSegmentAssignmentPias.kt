package org.janelia.saalfeldlab.paintera.control.assignment

import gnu.trove.map.TLongIntMap
import gnu.trove.map.TLongObjectMap
import gnu.trove.map.hash.TLongIntHashMap
import gnu.trove.map.hash.TLongLongHashMap
import gnu.trove.map.hash.TLongObjectHashMap
import gnu.trove.set.hash.TLongHashSet
import net.imglib2.type.label.Label
import net.imglib2.util.StopWatch
import org.janelia.saalfeldlab.fx.ObservableWithListenersList
import org.janelia.saalfeldlab.paintera.control.assignment.action.AssignmentAction
import org.janelia.saalfeldlab.paintera.control.assignment.action.Detach
import org.janelia.saalfeldlab.paintera.control.assignment.action.Merge
import org.janelia.saalfeldlab.paintera.id.IdService
import org.janelia.saalfeldlab.paintera.util.zmq.sockets.*
import org.slf4j.LoggerFactory
import org.zeromq.ZMQ
import java.io.Closeable
import java.lang.invoke.MethodHandles
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.util.*
import java.util.function.LongSupplier
import kotlin.math.max
import kotlin.math.min
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

    object solutionState {
        val SUCCESS                       = 0
        val NO_LABEL_FOR_SOME_CLASSES     = 1
        val RANDOM_FOREST_TRAINING_FAILED = 2
        val MC_OPTIMIZATION_FAILED        = 3
        val UNKNOWN_ERRROR                = 4
    }

    object requestSolutionUpdate {
        object responseCodes {
            val _SOLUTION_UPDATE_REQUEST_RECEIVED = 0
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

class SolutionUpdateSubscriber(
        context: ZMQ.Context,
        val address: String,
        val onNewSolution: (Int, Int) -> Any,
        recvTimeout: Int = -1): Thread(), Closeable {

    val socket: ZMQ.Socket
    private var keepListening = true

    init {
        isDaemon = true
        socket = subscriberSocket(context, address, receiveTimeout = recvTimeout)
        start()
    }

    override fun run() {
        while(keepListening) {
            val newSolution = socket.recv()?.toInts()
            if (newSolution != null && newSolution.size == 2)
                onNewSolution(newSolution[0], newSolution[1])
        }
    }

    override fun close() {
        keepListening = false
    }

}

class FragmentSegmentAssignmentPias(
        val piasAddress: String,
        val idService: IdService,
        val context: ZMQ.Context,
        lastFragmentLabel: TLongIntHashMap = TLongIntHashMap(),
        examples: TLongObjectHashMap<TLongIntHashMap> = TLongObjectHashMap()
        ): ObservableWithListenersList(), FragmentSegmentAssignmentState, Closeable {

    private val examples = TLongObjectHashMap<TLongIntHashMap>()
    private val lastFragmentLabel = TLongIntHashMap()
    private val positiveExamples = TLongLongHashMap()
    private val negativeExamples = TLongLongHashMap()
    private val newSolutionListeners = mutableListOf<(Int) -> Any>()
    private val solutionSubscriber = SolutionUpdateSubscriber(context, newSolutionSubscriptionAddress(), this::notifyNewSolution, recvTimeout = 50)

    var updateOnNewSolution = true
    var requestSolutionUpdateOnCommit = false

    private var lookup = Lookup()

    private inner class Lookup(assignments: TLongLongHashMap = TLongLongHashMap()) {

        val fragmentSegment = TLongLongHashMap()
        val segmentFragment = TLongObjectHashMap<TLongHashSet>()

        init {
//            val counts = TLongLongHashMap()
//            assignments.forEachValue{ counts.put(it, counts[it] + 1); true }
//            val rootMapping = TLongLongHashMap()
//            assignments.forEachEntry { k, v ->
//                if (counts[v] > 1) {
//                    if (!rootMapping.containsKey(v)) {
//                        val root = idService.next()
//                        rootMapping.put(v, root)
//                        segmentFragment.put(root, TLongHashSet())
//                    }
//                    val root = rootMapping[v]
//                    fragmentSegment.put(k, root)
//                    segmentFragment[root].add(k)
//                }
//                true
//            }
            fragmentSegment.putAll(assignments)
            // scoped extension functions, so cool!
            with(TroveExtensions) {
                fragmentSegment.forEachEntry { k, v -> segmentFragment.computeIfAbsent(v) { TLongHashSet() }.add(k) }
            }
        }
    }

    init {
        this.lastFragmentLabel.putAll(lastFragmentLabel)
        this.examples.putAll(examples)
        examples.forEachEntry { e1, value -> value.forEachEntry { e2, v -> (if (v==1) positiveExamples else negativeExamples).put(e1, e2);true }; true }
        newSolutionListeners.add { if (updateOnNewSolution) updateSolution(recvTimeout = 5000, sendTimeout = 5000) }
    }

    override fun close() {
        solutionSubscriber.close()
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
        LOG.debug("Applying action {}", action)
        action.let {
            if (it is Merge)
                listOf(LabeledEdge(it.fromFragmentId, it.intoFragmentId, 1)) else if (it is Detach)
                listOf(LabeledEdge(it.fragmentId, it.fragmentFrom, 0)) else
                null
        }?.let { setEdgeLabels(it, recvTimeout = 1000, sendTimeout = 1000); requestUpdateSolution(recvTimeout = 1000, sendTimeout = 1000) }
    }

    override fun apply(actions: MutableCollection<out AssignmentAction>?) {
        LOG.debug("Applying actions {}", actions)
        actions?.forEach { apply(it) }
    }

    override fun getMergeAction(from: Long, into: Long, newSegmentId: LongSupplier?): Optional<Merge> {
        if (from == into)
            return Optional.empty()
        return Optional.of(Merge(min(from, into), max(from, into), Label.INVALID))
    }

    override fun getDetachAction(fragmentId: Long, from: Long): Optional<Detach> {
        if (from == fragmentId)
            return Optional.empty()
        return Optional.of(Detach(min(from, fragmentId), max(from, fragmentId)))
    }

    private fun updateLookup(assignments: TLongLongHashMap) {
        LOG.debug("New lookup table arrived! {}", assignments)
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

    fun persistGroundTruthLabels(recvTimeout: Int = -1, sendTimeout: Int = -1): Pair<Boolean, Any?> {
        return clientSocket(context, apiAddress(), receiveTimeout = recvTimeout, sendTimeout = sendTimeout).use {
            it.send("/api/save-ground-truth-labels")
            val success = it.recv()?.toInt()
            val numMessages = it.recv()?.toInt()
            if (numMessages == 0 || numMessages == null)
                Pair(false, null)
            else if (success == 0 && numMessages == 1) {
                val messageType = it.recv()?.toInt()
                val message = it.recv()
                // 2 is type int, 0 is type string
                if (messageType == 2)
                    Pair(true, message?.toInt())
                else
                    Pair(false, message?.takeIf { messageType == 0 }?.let { String(it) } ?: message)
            } else {
                val messages = (0 until numMessages).map {_ ->
                    val messageType = it.recv()?.toInt()
                    it.recv()?.let { message ->
                        when (messageType) {
                            0 -> String(message)
                            2 -> message.toInt()
                            else -> message
                        }
                    }
                }
                Pair(false, messages)
            }
        }
    }

    private fun bytesToLookup(bytes: ByteArray) {
        LOG.trace("Received new lookup as bytes: {}", bytes)
        require(bytes.size % (1 * java.lang.Long.BYTES) == 0) {"Received byte array that is not integer multiple of long: ${bytes.size} -- ${Arrays.toString(bytes)}"}
        updateLookup(ByteBuffer.wrap(bytes).let {
            val map = TLongLongHashMap()
            (0 until bytes.size / java.lang.Long.BYTES).forEach { id -> map.put(id.toLong(), it.long) }
            LOG.debug("Updating lookup to {}", map)
            map
        })

    }

    private fun notifyNewSolution(solutionId: Int, exitCode: Int) {
        when (exitCode) {
            codes.solutionState.SUCCESS -> newSolutionListeners.forEach { it(solutionId) }
            else -> LOG.info("Latest solution failed with code {}", exitCode)
        }
    }

    fun addNewSolutionListener(listener: (Int) -> Any) = this.newSolutionListeners.add(listener)

    // TODO maybe throw exception is more informative than true/false
    fun updateSolution(recvTimeout: Int = -1, sendTimeout: Int = -1): Boolean {
        val socket = context.socket(ZMQ.REQ)
        socket.sendTimeOut = sendTimeout
        socket.receiveTimeOut = recvTimeout
        val solutionRequestAddress = requestCurrentSolutionAddress()
        socket.connect(solutionRequestAddress)
        LOG.debug("Requesting current solution at {}", solutionRequestAddress)
        socket.send("")
        return socket.recv()?.let {
            val responseCode = ByteBuffer.wrap(it).int
            LOG.debug(("Received response code $responseCode"))
            when(responseCode) {
                0 -> {bytesToLookup(socket.recv());true}
                1 -> false
                else -> false
            }
        } ?: false
    }

    fun requestUpdateSolution(recvTimeout: Int = -1, sendTimeout: Int = -1): Int? {
        val socket = clientSocket(context, address = requestSolutionUpdateAddress(), receiveTimeout = recvTimeout, sendTimeout = sendTimeout)
        socket.send("")
        val responseCode = socket.recv()?.toInt() ?: Exception("Did not receive response within $recvTimeout")
        when (responseCode) {
            codes.requestSolutionUpdate.responseCodes._SOLUTION_UPDATE_REQUEST_RECEIVED -> {socket.recv()!!.toInt().let{LOG.debug("Server received solution request and will generate new solution with id $it"); it}}
            else -> throw Exception("Do not understand response code $responseCode")
        }
        return null
    }

    fun setEdgeLabels(labels: Collection<LabeledEdge>, recvTimeout: Int = -1, sendTimeout: Int = -1) {

        LOG.debug("Setting edge labels {}", labels)
        createSocket(context, ZMQ.REQ, recvTimeout = recvTimeout, sendTimeout = sendTimeout).use { socket ->
            val address = setEdgeLabelsAddress()
            LOG.debug("Connecting set edge labels socket to $address")
            socket.connect(address)
            socket.send(codes.setEdge.requestCodes._SET_EDGE_REQ_EDGE_LIST.toBytes(), ZMQ.SNDMORE)
            // 20 = 8 + 8 + 4 bytes; plus one int for method
            val bytes = ByteArray(20 * labels.size)
            ByteBuffer.wrap(bytes).let { labels.forEach { le -> le.serializeToBuffer(it) } }
            socket.send(bytes, 0)
            val responseCode = socket.recv()?.let { ByteBuffer.wrap(it).int }
                    ?: throw Exception("Response code was null!")

            when (responseCode) {
                codes.setEdge.responseCodes._SET_EDGE_REP_DO_NOT_UNDERSTAND -> throw Exception("Server did not understand method ${socket.recv()?.let { ByteBuffer.wrap(it).int }}")
                codes.setEdge.responseCodes._SET_EDGE_REP_EXCEPTION -> throw Exception("Server threw exception when trying to add labeled edges: ${socket.recvStr(Charset.defaultCharset())}")
                codes.setEdge.responseCodes._SET_EDGE_REP_SUCCESS -> {
                }// TODO update edges in here
                else -> throw Exception("Do not understand response code $responseCode")
            }
        }
    }

    fun setEdgeLabels(labelMap: TLongObjectMap<TLongIntMap>, recvTimeout: Int = -1, sendTimeout: Int = -1) {
        val labeledEdges = mutableListOf<LabeledEdge>()
        labelMap.forEachEntry { e1, labels -> labels.forEachEntry { e2, label -> labeledEdges.add(LabeledEdge(e1, e2, label)); true }; true }
        setEdgeLabels(labeledEdges, recvTimeout = recvTimeout, sendTimeout = sendTimeout)
    }

    fun pingAddress() = "$piasAddress-ping"
    fun requestCurrentSolutionAddress() = "$piasAddress-current-solution"
    fun setEdgeLabelsAddress() = "$piasAddress-set-edge-labels"
    fun newSolutionSubscriptionAddress() = "$piasAddress-new-solution"
    fun requestSolutionUpdateAddress() = "$piasAddress-update-solution"
    fun apiAddress() = apiAddress(piasAddress)

    companion object {
        private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

        private fun createSocket(context: ZMQ.Context, type: Int, recvTimeout: Int = -1, sendTimeout: Int = -1): ZMQ.Socket {
            val socket = context.socket(type)
            socket.receiveTimeOut = recvTimeout
            socket.sendTimeOut = sendTimeout
            return socket
        }

        fun pingAddress(piasAddress: String) = "$piasAddress-ping"
        fun requestCurrentSolutionAddress(piasAddress: String) = "$piasAddress-current-solution"
        fun setEdgeLabelsAddress(piasAddress: String) = "$piasAddress-set-edge-labels"
        fun newSolutionSubscriptionAddress(piasAddress: String) = "$piasAddress-new-solution"
        fun requestSolutionUpdateAddress(piasAddress: String) = "$piasAddress-update-solution"
        fun apiAddress(piasAddress: String) = piasAddress
    }

    private class TroveExtensions {

        companion object {
            inline fun <T> TLongObjectMap<T>.computeIfAbsent(key: Long, mappingFunction: (Long) -> T) = this[key]
                    ?: putAndReturn(key, mappingFunction(key))

            fun <T> TLongObjectMap<T>.putAndReturn(key: Long, value: T): T {
                this.put(key, value)
                return value
            }
        }
    }



}

fun main(argv: Array<String>) {
    val context = ZMQ.context(10)
    val assignment = FragmentSegmentAssignmentPias("ipc:///tmp/pias", IdService.dummy(), context = context)
    assignment.addNewSolutionListener { println("New solution with id $it available") }
    assignment.addNewSolutionListener { println("updated solution within 0.5s? ${assignment.updateSolution(recvTimeout = 500)}") }
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
    assignment.requestUpdateSolution(recvTimeout = 100)
//    val edges = arrayOf(LabeledEdge(1, 2, 0), LabeledEdge(2, 3, 1))
    // (67, 170): 0, (67, 4259)
    val edges = arrayOf(LabeledEdge(67, 170, 0), LabeledEdge(67, 4259, 1))
    assignment.setEdgeLabels(edges.toList())
    assignment.requestUpdateSolution(recvTimeout = 1000)
    Thread.sleep(5000)


}