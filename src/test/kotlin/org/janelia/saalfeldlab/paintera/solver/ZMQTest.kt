package org.janelia.saalfeldlab.paintera.solver

import com.google.gson.Gson
import com.google.gson.GsonBuilder
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
import java.util.function.Supplier

class ZMQTest {

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

        val ctx = ZMQ.context(1)
        val latestSolutionRequestSocket = ctx.socket(ZMQ.REP)
        latestSolutionRequestSocket.bind(LATEST_SOL_ADDR)

        val currentSolutionSocket = ctx.socket(ZMQ.REQ)
        currentSolutionSocket.connect(LATEST_SOL_ADDR)

        val currentSolutionRequest = Supplier {
            LOG.warn("Receiving solution request")
            val request = latestSolutionRequestSocket.recv(0)
            LOG.warn("Received request: {}", request)
            try {
                null as? Void
            }
            finally {
                LOG.warn("Returned null as Void")
            }
        }

        val t = Thread( {
            val message = currentSolutionRequest.get()
            LOG.warn("Received message ${message}")
        } )
        t.start()

        currentSolutionSocket.send("")

        t.join()



    }
}