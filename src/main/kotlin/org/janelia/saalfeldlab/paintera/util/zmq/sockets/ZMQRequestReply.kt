package org.janelia.saalfeldlab.paintera.util.zmq.sockets

import org.zeromq.ZMQ


fun serverSocket(context: ZMQ.Context, address: String, receiveTimeout: Int = -1, sendTimeout: Int = -1): ZMQ.Socket {
    val socket = context.socket(ZMQ.REP)
    socket.receiveTimeOut = receiveTimeout
    socket.sendTimeOut = sendTimeout
    socket.bind(address)
    return socket
}

fun clientSocket(context: ZMQ.Context, address: String, receiveTimeout: Int = -1, sendTimeout: Int = -1): ZMQ.Socket {
    val socket = context.socket(ZMQ.REQ)
    try {
        socket.receiveTimeOut = receiveTimeout
        socket.sendTimeOut = sendTimeout
        socket.connect(address)
        return socket
    } catch (e: RuntimeException) {
        socket.close()
        return socket;
    }
}



