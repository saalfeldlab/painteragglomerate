package org.janelia.saalfeldlab.paintera.solver

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.JsonArray
import com.google.gson.JsonObject
import gnu.trove.list.array.TLongArrayList
import gnu.trove.map.hash.TLongLongHashMap
import org.janelia.saalfeldlab.paintera.control.assignment.action.AssignmentAction
import org.janelia.saalfeldlab.paintera.control.assignment.action.Detach
import org.janelia.saalfeldlab.paintera.control.assignment.action.Merge
import org.junit.Assert
import org.junit.Test
import org.slf4j.LoggerFactory
import org.zeromq.ZMQ
import java.io.IOException
import java.lang.invoke.MethodHandles
import java.nio.ByteBuffer
import java.util.*

class SolverQueueServerZMQTest {


    companion object {

        private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

        private val ACTION_ADDRESS = "ipc://ACTION"

        private val SOLUTION_REQ_REP = "ipc://SOL_REQ_REP"

        private val SOLUTION_DIST = "ipc://SOL_DIST"

        private val LATEST_SOL_ADDR = "ipc://LATEST_SOL"

        private val MIN_WAIT_AFTER_LAST_ACTION = 100

        private val INITIAL_SOLUTION = { TLongLongHashMap(longArrayOf(4), longArrayOf(2)) }

        private val gson: Gson = GsonBuilder()
                .registerTypeAdapter(AssignmentAction::class.java, AssignmentActionJsonAdapter())
                .create()
    }

    @Test
    @Throws(InterruptedException::class, IOException::class)
    fun test() {
        val solution = INITIAL_SOLUTION()
        LOG.debug("Got initial solution {}", solution)

        val solutionHandlerThread = Thread {
            Thread.currentThread().name = "solution-handler"
            val actions = ArrayList<AssignmentAction>()

            val ctx = ZMQ.context(1)
            val socket = ctx.socket(ZMQ.REP)
            socket.bind(SOLUTION_REQ_REP)

            while (!Thread.interrupted()) {

                LOG.warn("Waiting for message in thread {}", Thread.currentThread().name)
                val msg = socket.recvStr()
                LOG.warn("Received message in thread {}", Thread.currentThread().name)
                //				System.out.println( "RECEIVED MSG " + msg );

                val incomingActions = SolverQueueServerZMQ.deserialize(msg, gson)
                actions.addAll(incomingActions)

                for (action in incomingActions)
                    if (action is Merge) {
                        val merge = action as Merge
                        val ids = Pair(merge.fromFragmentId, merge.intoFragmentId)
//                        for (i in ids.indices)
//                            for (m in i + 1 until ids.size) {
                        val f1 = merge.fromFragmentId
                        val f2 = merge.intoFragmentId

                        if (!solution.contains(f1))
                            solution.put(f1, f1)

                        if (!solution.contains(f2))
                            solution.put(f2, f2)

                        val s1 = solution.get(f1)
                        val s2 = solution.get(f2)
                        val s = Math.min(s1, s2)

                        val fs = TLongArrayList()
                        solution.forEachEntry({ k, v ->
                            if (v == s1 || v == s2)
                                fs.add(k)
                            true
                        })
                        fs.forEach { id ->
                            solution.put(id, s)
                            true
                        }
//                            }

                    } else if (action is Detach) {
                        val detach = action as Detach
                        val id = detach.fragmentId
                        if (solution.contains(id))
                            solution.put(id, id)
                    }

                val maxLabel = Arrays.stream(solution.keys()).reduce({ l, l1 -> java.lang.Long.max(l, l1) }).getAsLong()
                val mapping = LongArray((maxLabel + 1).toInt())
                val response = ByteArray(mapping.size * java.lang.Long.BYTES)
                Arrays.fill(mapping, -1)
                val responseBuffer = ByteBuffer.wrap(response)
                solution.forEachEntry({ k, v ->
                    mapping[k.toInt()] = v
                    true
                })
                for (m in mapping)
                    responseBuffer.putLong(m)
                socket.send(response, 0)

            }

            socket.close()
            ctx.close()

        }
        solutionHandlerThread.start()

        val server = SolverQueueServerZMQ(
                ACTION_ADDRESS,
                SOLUTION_REQ_REP,
                SOLUTION_DIST,
                SolverQueueServerZMQ.makeJavaSupplier(INITIAL_SOLUTION),
                LATEST_SOL_ADDR,
                1,
                MIN_WAIT_AFTER_LAST_ACTION.toLong())
        LOG.warn("Started server")

        val ctx = ZMQ.context(1)
        val currentSolutionSocket = ctx.socket(ZMQ.REQ)
        currentSolutionSocket.connect(LATEST_SOL_ADDR)
        LOG.warn("Sending empty message")
        currentSolutionSocket.send("")
        LOG.warn("Sent empty message")
        val solutionResponse = currentSolutionSocket.recv()
        LOG.warn("Received solution={}", solutionResponse)

        Assert.assertNotNull(solutionResponse)
        Assert.assertEquals(0, solutionResponse.size % (2 * java.lang.Long.BYTES))

        val init = INITIAL_SOLUTION()
        LOG.warn("init={}", init)

        val responseHM = TLongLongHashMap()

        val solutionBuffer = ByteBuffer.wrap(solutionResponse)
        while (solutionBuffer.hasRemaining())
            responseHM.put(solutionBuffer.long, solutionBuffer.long)
        Assert.assertEquals(init.size() * 2 * java.lang.Long.BYTES, solutionResponse.size)

        Assert.assertEquals(init, responseHM)

        currentSolutionSocket.close()

        val actionSocket = ctx.socket(ZMQ.REQ)
        val subscriptionSocket = ctx.socket(ZMQ.SUB)
        actionSocket.connect(ACTION_ADDRESS)
        subscriptionSocket.connect(SOLUTION_DIST)
        subscriptionSocket.subscribe("".toByteArray())
        val solutionSubscription = TLongLongHashMap()
        val solutionSubscriptionThread = Thread {
            Thread.currentThread().name = "solutionSubscriptionThread"
            LOG.warn("Waiting for message")
            val msg = subscriptionSocket.recv()
            LOG.warn("Received message ${msg}")
            Assert.assertEquals(0, msg.size % (2 * java.lang.Long.BYTES))
            solutionSubscription.clear()
            val bb = ByteBuffer.wrap(msg)
            while (bb.hasRemaining()) {
                val k = bb.long
                val v = bb.long
                if (v >= 0)
                    solutionSubscription.put(k, v)
            }
        }
        solutionSubscriptionThread.start()

        val testActions = ArrayList<AssignmentAction>()
        testActions.add(Merge(3, 1, 1))
        testActions.add(Merge(3, 2, 2))
        testActions.add(Detach(5, 1))
        testActions.add(Detach(3, 1))
        val testActionsJsonArray = JsonArray()
        for (ta in testActions) {
            testActionsJsonArray.add(gson.toJsonTree(ta, AssignmentAction::class.java))
        }

        val jsonObject = JsonObject()
        jsonObject.add("actions", testActionsJsonArray)
        jsonObject.addProperty("version", "1")

        actionSocket.send(jsonObject.toString())
        val response = actionSocket.recv()
        LOG.warn("Received response from actionSocket {}", response)
        Assert.assertEquals(1, response.size)
        Assert.assertEquals(0, response[0].toInt())
        Thread.sleep(300)

        LOG.warn("Joining solutionSubscriptionThread")
        solutionSubscriptionThread.join()

        LOG.warn("Closing actionSocket")
        actionSocket.close()

        LOG.warn("Closing subscriptionSocket")
        subscriptionSocket.close()

        LOG.warn("Closing context")
        ctx.close()

        LOG.warn("Interrupting solutionHandlerThread")
        solutionHandlerThread.interrupt()

        val solutionNormalized = TLongLongHashMap()
        val minimumLabelInSegmentMap = TLongLongHashMap()
        solution.forEachEntry({ k, v ->

            if (v >= 0)
                if (!minimumLabelInSegmentMap.contains(v))
                    minimumLabelInSegmentMap.put(v, k)
                else
                    minimumLabelInSegmentMap.put(v, Math.min(minimumLabelInSegmentMap.get(v), k))
            true
        })

        solution.forEachEntry({ k, v ->
            solutionNormalized.put(k, minimumLabelInSegmentMap.get(v))
            true
        })
        val solutionReference = TLongLongHashMap(
                longArrayOf(4, 3, 2, 1),
                longArrayOf(1, 3, 1, 1))
        Assert.assertEquals(solutionReference, solutionNormalized)
        Assert.assertEquals(solutionReference, solutionSubscription)

    }

}
