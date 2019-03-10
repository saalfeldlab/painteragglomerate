package org.janelia.saalfeldlab.paintera

import org.janelia.saalfeldlab.paintera.data.n5.N5FSMeta
import org.janelia.saalfeldlab.paintera.util.zmq.sockets.clientSocket
import org.janelia.saalfeldlab.paintera.util.zmq.sockets.toInt
import org.slf4j.LoggerFactory
import org.zeromq.ZMQ
import java.lang.invoke.MethodHandles
import java.nio.charset.Charset

class PiasContainer {
    companion object {

        private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

        private val N5_META_ENDPOINT = "/api/n5/all"

        @Throws(PiasEndpointException::class)
        fun n5MetaFromPias(
                context: ZMQ.Context,
                address: String,
                recvTimeout: Int = - 1,
                sendTimeout: Int = -1): N5FSMeta?  {

            val socket = try {
                clientSocket(context, address, receiveTimeout = recvTimeout, sendTimeout = sendTimeout)
            } catch (e: Exception) {
                LOG.error("Unable to get n5 meta information from PIAS server at {}: {}", address, e.message)
                null
            }

            return socket?.let {
                it.send(N5_META_ENDPOINT)
                // returnCode success: 0
                val returnCode = it.recv().toInt()
                val success = returnCode == 0
                val numMessages = it.recv().toInt()
                if (success)
                    require(numMessages == 2) { "Did expect 2 messages but got $numMessages" }
                val n5metaList = mutableListOf<String>()
                val messages = (0 until numMessages).map {index ->
                    val messageType = it.recv().toInt()
                    if (success) {
                        // messageType == 0: string
                        require(messageType == 0) { "Message type $messageType not consistent with expected type 0"}
                        val msg = it.recvStr(Charset.defaultCharset())
                        n5metaList.add(msg)
                        msg
                    } else
                        it.recv()
                }
                it.close()
                if (!success)
                    throw PiasEndpointException(address, returnCode, *messages.toTypedArray(), "Request to $address/${N5_META_ENDPOINT} returned non-successfully")
                N5FSMeta(n5metaList[0], n5metaList[1])
            }
        }



    }
}