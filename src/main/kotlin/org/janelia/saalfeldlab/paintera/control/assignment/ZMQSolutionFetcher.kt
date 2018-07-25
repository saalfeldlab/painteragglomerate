package org.janelia.saalfeldlab.paintera.control.assignment

import gnu.trove.map.hash.TLongLongHashMap
import org.janelia.saalfeldlab.paintera.solver.SolverQueueServerZMQ
import org.slf4j.LoggerFactory
import org.zeromq.ZMQ
import java.io.Closeable
import java.lang.invoke.MethodHandles
import java.util.function.Supplier

class ZMQSolutionFetcher(context: ZMQ.Context, val address: String) : Supplier<TLongLongHashMap>, Closeable {

    companion object {
        private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

        private fun createSocket(context: ZMQ.Context, address: String): ZMQ.Socket {
            val socket = context.socket(ZMQ.REQ)
            socket.connect(address)
            return socket
        }
    }

    val socket = createSocket(context, address)

    override fun get(): TLongLongHashMap {
        synchronized(socket)
        {
            LOG.warn("Sending solution request to {}", address)
            socket.send("")
            LOG.warn("Waiting for response")
            val response = socket.recv()
            LOG.warn("Got response {}", response)
            val map = SolverQueueServerZMQ.fromBytes(response)
            return map
        }
    }

    override fun close() {
        socket.close()
    }
}