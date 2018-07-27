package org.janelia.saalfeldlab.paintera.id

import org.slf4j.LoggerFactory
import org.zeromq.ZMQ
import java.io.Closeable
import java.lang.invoke.MethodHandles
import java.nio.ByteBuffer
import java.util.stream.LongStream

/**
 *
 * sends 1 + N byte:
 * first byte is type of message
 * INVALIDATE:     N=8 -> id (long) to invalidate
 * NEXT:           N=4 -> number (int) of requested ids
 * IS_INVALIDATED: N=8 -> id (long) to be checked
 *
 * receives N byte:
 * INVALIDATE:     N=0
 * NEXT:           N=16 -> [start, stop[ for range of requested ids
 * IS_INVALIDATED: N=1  -> >0 if invalidated, else 0
 */
class IdSelectorZMQ(
        idRequestAddress: String,
        context: ZMQ.Context
) : IdService, Closeable {

    enum class Message(val type: Byte) {
        INVALIDATE(0x01),
        NEXT(0x02),
        IS_INVALIDATED(0x03)
    }


    val nextIdRequestSocket = requestSocket(context, idRequestAddress)

    override fun invalidate(id: Long) {
        val data = ByteArray(1 + java.lang.Long.BYTES)
        val bb = ByteBuffer.wrap(data)
        bb.put(Message.INVALIDATE.type)
        bb.putLong(id)
        synchronized(nextIdRequestSocket)
        {
            val sentSuccessfully = nextIdRequestSocket.send(data, 0)
            LOG.warn("Sent message {} successfully? {}", data, sentSuccessfully)
            nextIdRequestSocket.recv()
        }
    }

    override fun next(): Long {
        return next(1).get(0)
    }

    override fun next(n: Int): LongArray {
        val data = ByteArray(1 + java.lang.Integer.BYTES)
        val bb = ByteBuffer.wrap(data)
        bb.put(Message.NEXT.type)
        bb.putInt(n)
        synchronized(nextIdRequestSocket)
        {
            val sentSuccessfully = nextIdRequestSocket.send(data, 0)
            LOG.warn("Sent message {} successfully? {}", data, sentSuccessfully)
            val response = nextIdRequestSocket.recv()
            LOG.warn("Received response {}", response)
            val responseBB = ByteBuffer.wrap(response)
            return LongStream.range(responseBB.long, responseBB.long).toArray()
        }
    }

    override fun isInvalidated(id: Long): Boolean {
        val data = ByteArray(1 + java.lang.Long.BYTES)
        val bb = ByteBuffer.wrap(data)
        bb.put(Message.IS_INVALIDATED.type)
        bb.putLong(id)
        synchronized(nextIdRequestSocket)
        {
            val sentSuccessfully = nextIdRequestSocket.send(data, 0)
            LOG.warn("Sent message {} successfully? {}", data, sentSuccessfully)
            val response = nextIdRequestSocket.recv()
            LOG.warn("Received response {}", response)
            return response.get(0) > 0
        }
    }

    override fun close() {
        nextIdRequestSocket.close()
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

        private fun requestSocket(context: ZMQ.Context, address: String): ZMQ.Socket {
            val socket = context.socket(ZMQ.REQ)
            socket.connect(address)
            return socket
        }
    }
}
