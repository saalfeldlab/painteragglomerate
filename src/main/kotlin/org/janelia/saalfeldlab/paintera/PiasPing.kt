package org.janelia.saalfeldlab.paintera

import javafx.beans.property.ReadOnlyBooleanProperty
import javafx.beans.property.ReadOnlyLongProperty
import javafx.beans.property.ReadOnlyProperty
import javafx.beans.property.SimpleBooleanProperty
import javafx.beans.property.SimpleLongProperty
import javafx.beans.property.SimpleObjectProperty
import javafx.beans.value.ObservableBooleanValue
import org.janelia.saalfeldlab.paintera.util.zmq.sockets.clientSocket
import org.slf4j.LoggerFactory
import org.zeromq.ZMQ
import java.io.Closeable
import java.lang.invoke.MethodHandles
import java.nio.charset.Charset

class PiasPing(
        context: ZMQ.Context,
        val piasAddress: String,
        recvTimeout: Int,
        sendTimeout: Int): Closeable {

    private val socket = pingSocket(context, recvTimeout = recvTimeout, sendTimeout = sendTimeout)!!

    private val lastSuccessfulPing = SimpleLongProperty(System.nanoTime())
    private val pingSuccessful = SimpleBooleanProperty(false)

    private var isClosed = false

    private val pingThread = Thread(Runnable {
        while (!isClosed) {
            val pong = pingServer(socket)
            if (pong == null)
                pingSuccessful.value = false
            else {
                pingSuccessful.value = true
                lastSuccessfulPing.value = System.nanoTime()
            }
        }
    }).let { it.isDaemon = false; it.start(); it }

    fun lastSuccessfulPingProperty() = lastSuccessfulPing as ReadOnlyLongProperty
    fun pingSuccesfulProperty() = pingSuccessful as ReadOnlyBooleanProperty


    private fun pingSocket(context: ZMQ.Context, recvTimeout: Int = -1, sendTimeout: Int = -1): ZMQ.Socket? {
        return try {
            clientSocket(context, pingAddress(), recvTimeout, sendTimeout)
        } catch (e: Exception) {
            null
        }
    }

    private fun pingAddress() = Companion.pingAddress(piasAddress)

    private fun pingServer(socket: ZMQ.Socket): String? {
        return socket
                .recvStr(Charset.defaultCharset())
                .let { LOG.debug("Received pong from {}: `{}'", pingAddress(), it); it }
    }

    override fun close() {
        isClosed = true
        pingThread.join()
        socket.close()
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

        private fun pingAddress(piasAddress: String) = "$piasAddress-ping"
    }
}