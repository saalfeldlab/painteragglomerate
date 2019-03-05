package org.janelia.saalfeldlab.paintera.util.zmq.sockets

import java.nio.ByteBuffer

fun Int.toBytes(buffer: ByteBuffer) = buffer.putInt(this)

fun Int.toBytes(bytes: ByteArray) = toBytes(ByteBuffer.wrap(bytes))

fun Int.toBytes() = ByteArray(java.lang.Integer.BYTES).let { toBytes(it); it }

fun IntArray.toBytes(buffer: ByteBuffer) = this.forEach { it.toBytes(buffer) }

fun IntArray.toBytes(bytes: ByteArray) = toBytes(ByteBuffer.wrap(bytes))

fun IntArray.toBytes() = ByteArray(java.lang.Integer.BYTES * size).let { toBytes(it); it }