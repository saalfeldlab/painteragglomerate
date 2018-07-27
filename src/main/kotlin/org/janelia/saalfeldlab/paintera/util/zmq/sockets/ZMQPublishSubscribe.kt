package org.janelia.saalfeldlab.paintera.util.zmq.sockets

import org.slf4j.LoggerFactory
import org.zeromq.ZMQ
import java.lang.invoke.MethodHandles
import java.nio.charset.Charset

private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

fun subscriberSocket(context: ZMQ.Context, address: String, topic: String = "", receiveTimeout: Int = -1): ZMQ.Socket {
    val subscriber = context.socket(ZMQ.SUB)
    subscriber.receiveTimeOut = receiveTimeout
    subscriber.connect(address)
    subscriber.subscribe(topic.toByteArray(Charset.defaultCharset()))
    LOG.debug("Creating susbcriber for topic {} at address {}", topic, address)
    return subscriber
}

fun publisherSocket(context: ZMQ.Context, address: String): ZMQ.Socket {
    val publisher = context.socket(ZMQ.PUB)
    publisher.bind(address)
    LOG.debug("Creating publisher at address {}", address)
    return publisher
}


