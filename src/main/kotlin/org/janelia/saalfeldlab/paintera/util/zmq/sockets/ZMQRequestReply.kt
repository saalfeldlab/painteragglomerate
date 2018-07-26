package org.janelia.saalfeldlab.paintera.util.zmq.sockets

import org.zeromq.ZMQ


fun serverSocket(context: ZMQ.Context, address: String): ZMQ.Socket {
    val socket = context.socket(ZMQ.REP)
    socket.connect(address)
    return socket
}

fun clientSocket(context: ZMQ.Context, address: String): ZMQ.Socket {
    val socket = context.socket(ZMQ.REQ)
    socket.connect(address)
    return socket
}



