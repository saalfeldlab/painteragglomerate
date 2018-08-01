package org.janelia.saalfeldlab.paintera.util.zmq

import gnu.trove.map.TLongLongMap
import gnu.trove.map.hash.TLongLongHashMap
import java.nio.ByteBuffer

fun toMapFromSolverServer(data: ByteArray): TLongLongHashMap
{
    assert(isArraySizeValid(data))

    val bb = ByteBuffer.wrap(data).asLongBuffer()
    val map = TLongLongHashMap()
    for (i in 0 until bb.capacity())
    {
        map.put(i.toLong(), bb.get())
    }
    return map
}

fun toBytesFromMap(map: TLongLongMap): ByteArray
{
    val data = ByteArray(2 * java.lang.Long.BYTES * map.size())
    val bb = ByteBuffer.wrap(data)
    val it = map.iterator()
    while (it.hasNext())
    {
        it.advance()
        bb.putLong(it.key())
        bb.putLong(it.value())
    }
    return data
}

fun isArraySizeValid(data: ByteArray): Boolean
{
    return data.size % (2 * java.lang.Long.BYTES) == 0
}