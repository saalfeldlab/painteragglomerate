package org.janelia.saalfeldlab.paintera.solver

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.JsonObject
import com.google.gson.JsonParseException
import gnu.trove.map.TLongLongMap
import gnu.trove.map.hash.TLongLongHashMap
import org.janelia.saalfeldlab.paintera.control.assignment.action.AssignmentAction
import org.slf4j.LoggerFactory
import org.zeromq.ZMQ
import java.io.Closeable
import java.io.IOException
import java.lang.invoke.MethodHandles
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.util.*
import java.util.function.Consumer
import java.util.function.Supplier

class SolverQueueServerZMQ(
        actionReceiverAddress: String,
        solutionRequestResponseAddress: String,
        solutionDistributionAddress: String,
        initialSolution: Supplier<TLongLongHashMap>,
        latestSolutionRequestAddress: String,
        ioThreads: Int,
        minWaitTimeAfterLastAction: Long) : Closeable {

    companion object {
        private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

        fun deserialize(jsonString : String, gson : Gson) : List<AssignmentAction>
        {
            val json = gson.fromJson(jsonString, JsonObject::class.java)
            val actions = ArrayList<AssignmentAction>()
            for ( el in json.get("actions").asJsonArray )
            {
                actions.add(gson.fromJson(el, AssignmentAction::class.java))
            }

            return actions
        }

        private class ReceiveAction(val gson: Gson, val actionReceiverSocket: ZMQ.Socket) : Supplier<Iterable<AssignmentAction>>
        {
            override fun get(): Iterable<AssignmentAction> {
                LOG.debug("WAITING FOR MESSAGE IN ACTION RECEIVER at socket! {}", actionReceiverSocket)
                val message = this.actionReceiverSocket.recvStr(0, Charset.defaultCharset())
                LOG.debug("RECEIVED THE FOLLOWING MESSAGE: {}", message)

                if (message == null)
                    return ArrayList<AssignmentAction>()

                try {
                    val actions = deserialize(message, gson)

                    LOG.debug("RETURNING THESE ACTIONS: {}", actions)
                    return actions
                } catch (e: JsonParseException) {
                    return ArrayList<AssignmentAction>()
                }
            }

        }

        private class SolutionRequest(
                val gson : Gson,
                val solutionRequestResponseSocket: ZMQ.Socket
        ) : Consumer<Collection<AssignmentAction>>
        {
            override fun accept(actions: Collection<AssignmentAction>) {

                val json = JsonObject()
                json.addProperty("actions", gson.toJson(actions))
                this.solutionRequestResponseSocket.send(json.toString())
            }
        }

        private class ReceiveSolution(val solutionRequestResponseSocket: ZMQ.Socket) : Supplier<TLongLongHashMap>
        {
            override fun get(): TLongLongHashMap {
                val data = this.solutionRequestResponseSocket.recv()
                val numEntries = data.size / java.lang.Long.BYTES
                val keys = LongArray(numEntries)
                val values = LongArray(numEntries)
                val bb = ByteBuffer.wrap(data)
                for (i in 0 until numEntries) {
                    keys[i] = i.toLong()
                    values[i] = bb.long
                }
                return TLongLongHashMap(keys, values)
            }

        }

        private class RespondWithSolution(val latestSolutionRequestSocket : ZMQ.Socket) : Consumer<TLongLongMap>
        {
            override fun accept(solution: TLongLongMap) {
                this.latestSolutionRequestSocket.send(toBytes(solution), 0)
            }

        }

        private fun toBytes(map : TLongLongMap) : ByteArray
        {
            val keys = map.keys()
            val values = map.values()
            val data = ByteArray(java.lang.Long.BYTES * 2 * map.size())
            val bb = ByteBuffer.wrap(data)
            val iter = map.iterator()
            while(iter.hasNext())
            {
                iter.advance()
                bb.putLong(iter.key())
                bb.putLong(iter.value())
            }
            return data
        }

        private class DistributeSolution(val solutionDistributionSocket : ZMQ.Socket ) : Consumer<TLongLongHashMap>
        {
            override fun accept(solution: TLongLongHashMap) {
                LOG.debug("Sending solution: {}", solution)
                this.solutionDistributionSocket.send(toBytes(solution), 0)
            }
        }

        public fun makeJavaRunnable(f: () -> Unit): Runnable = object : Runnable {override fun run() {f()}}

        public fun <T> makeJavaSupplier(f: () -> T): Supplier<T> = object : Supplier<T> {override fun get() : T {return f()}}
    }

    private val ctx: ZMQ.Context

    private val actionReceiverSocket: ZMQ.Socket

    private val solutionRequestResponseSocket: ZMQ.Socket

    private val solutionDistributionSocket: ZMQ.Socket

    private val latestSolutionRequestSocket: ZMQ.Socket

    private val queue: SolverQueue

    private val gson: Gson = GsonBuilder()
            .registerTypeAdapter(AssignmentAction::class.java, AssignmentActionJsonAdapter())
            .create()

    init {
        this.ctx = ZMQ.context(ioThreads)

        this.actionReceiverSocket = ctx.socket(ZMQ.REP)
        this.actionReceiverSocket.bind(actionReceiverAddress)
        val actionReceiver = ReceiveAction(gson, actionReceiverSocket)

        val actionReceiptConfirmation = { this.actionReceiverSocket.send(byteArrayOf(0.toByte()), 0) }

        this.solutionRequestResponseSocket = ctx.socket(ZMQ.REQ)
        this.solutionRequestResponseSocket.connect(solutionRequestResponseAddress)

        val solutionRequester = SolutionRequest(gson, solutionRequestResponseSocket)

        val solutionReceiver = ReceiveSolution(solutionRequestResponseSocket)

        this.solutionDistributionSocket = ctx.socket(ZMQ.PUB)
        this.solutionDistributionSocket.bind(solutionDistributionAddress)

        val solutionDistributor = DistributeSolution(solutionDistributionSocket)

        this.latestSolutionRequestSocket = ctx.socket(ZMQ.REP)
        this.latestSolutionRequestSocket.bind(latestSolutionRequestAddress)

        val currentSolutionRequest = Supplier {
            LOG.warn("Receiving solution request")
            val request = this.latestSolutionRequestSocket.recv(0)
            LOG.warn("Received request: {}", request)
            try {
                null as? Void
            }
            finally {
                LOG.warn("Returned null as Void")
            }
        }
        val currentSolutionResponse = DistributeSolution(latestSolutionRequestSocket)

        this.queue = SolverQueue(
                actionReceiver,
                makeJavaRunnable { this.actionReceiverSocket.send(byteArrayOf(0.toByte()), 0) },
                solutionRequester,
                solutionReceiver,
                solutionDistributor,
                initialSolution,
                currentSolutionRequest,
                currentSolutionResponse,
                minWaitTimeAfterLastAction)
    }

    @Throws(IOException::class)
    override fun close() {
        queue.forceStop()
        actionReceiverSocket.close()
        solutionRequestResponseSocket.close()
        solutionDistributionSocket.close()
        ctx.close()

    }

}
