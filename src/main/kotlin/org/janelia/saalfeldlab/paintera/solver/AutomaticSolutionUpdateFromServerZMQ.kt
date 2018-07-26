package org.janelia.saalfeldlab.paintera.solver

import gnu.trove.map.hash.TLongLongHashMap
import org.janelia.saalfeldlab.paintera.util.zmq.sockets.subscriberSocket
import org.zeromq.ZMQ

class AutomaticSolutionUpdateFromServerZMQ(context: ZMQ.Context, address: String, topic: String="") : AutomaticSolutionUpdateFromServer {


    val socket = subscriberSocket(context, address, topic)

    override fun latestSolution(): TLongLongHashMap {
        val data = socket.recv()
        val map = TLongLongHashMap()

        return map
    }

}