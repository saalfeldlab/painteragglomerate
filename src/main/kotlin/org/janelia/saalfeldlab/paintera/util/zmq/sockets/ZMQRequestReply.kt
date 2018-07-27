package org.janelia.saalfeldlab.paintera.util.zmq.sockets

import org.zeromq.ZMQ


fun serverSocket(context: ZMQ.Context, address: String, receiveTimeout: Int = -1): ZMQ.Socket {
    val socket = context.socket(ZMQ.REP)
    socket.receiveTimeOut = receiveTimeout
    socket.bind(address)
    return socket
}

fun clientSocket(context: ZMQ.Context, address: String, receiveTimeout: Int = -1): ZMQ.Socket {
    val socket = context.socket(ZMQ.REQ)
    socket.receiveTimeOut = receiveTimeout
    socket.connect(address)
    return socket
}



