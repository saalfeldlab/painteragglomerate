package org.janelia.saalfeldlab.paintera.util.zmq.sockets

import org.zeromq.ZMQ
import java.io.Closeable
import java.util.concurrent.atomic.AtomicBoolean

open class CloseableSocket(val socket: ZMQ.Socket) : Closeable {

    val isClosed = AtomicBoolean(false)

    override fun close() {
        isClosed.set(true)
        socket.close()
    }


}