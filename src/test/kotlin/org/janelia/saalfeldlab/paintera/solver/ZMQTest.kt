package org.janelia.saalfeldlab.paintera.solver

import org.junit.Test
import org.slf4j.LoggerFactory
import org.zeromq.ZMQ
import java.io.IOException
import java.lang.invoke.MethodHandles
import java.util.function.Supplier

class ZMQTest {

    companion object {

        private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

        private val LATEST_SOL_ADDR = "ipc://LATEST_SOL"
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
                null as? Void?
            } finally {
                LOG.warn("Returned null as Void")
            }
        }

        val t = Thread({
            val message = currentSolutionRequest.get()
            LOG.warn("Received message ${message}")
        })
        t.start()

        currentSolutionSocket.send("")

        t.join()


    }
}