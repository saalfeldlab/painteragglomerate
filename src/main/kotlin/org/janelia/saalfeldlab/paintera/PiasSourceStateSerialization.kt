package org.janelia.saalfeldlab.paintera

import com.google.common.primitives.UnsignedLong
import com.google.gson.JsonDeserializationContext
import com.google.gson.JsonDeserializer
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import com.google.gson.JsonSerializationContext
import net.imglib2.type.NativeType
import net.imglib2.type.label.LabelMultisetType
import net.imglib2.type.label.VolatileLabelMultisetType
import net.imglib2.type.numeric.IntegerType
import net.imglib2.type.numeric.integer.LongType
import net.imglib2.type.numeric.integer.UnsignedIntType
import net.imglib2.type.numeric.integer.UnsignedLongType
import net.imglib2.type.volatiles.VolatileLongType
import net.imglib2.type.volatiles.VolatileUnsignedIntType
import net.imglib2.type.volatiles.VolatileUnsignedLongType
import org.janelia.saalfeldlab.paintera.exception.PainteraException
import org.janelia.saalfeldlab.paintera.serialization.PainteraSerialization
import org.janelia.saalfeldlab.paintera.serialization.StatefulSerializer
import org.janelia.saalfeldlab.paintera.state.SourceState
import org.scijava.plugin.Plugin
import org.slf4j.LoggerFactory
import java.lang.invoke.MethodHandles
import java.lang.reflect.Type
import java.util.function.IntFunction
import java.util.function.Supplier
import kotlin.reflect.KClass

private const val RESOLUTION_KEY = "resolution"
private const val OFFSET_KEY = "offset"
private const val PIAS_KEY = "pias"
private const val ADDRESS_KEY = "address"
private const val NAME_KEY = "name"
private const val DATATYPE_KEY = "dataType"

private typealias State = PiasSourceState<*, *>
private typealias CompatibleType = PiasSourceState.CompatibleType

@Plugin(type = PainteraSerialization.PainteraSerializer::class)
class PiasSerializer : PainteraSerialization.PainteraSerializer<State> {

    override fun serialize(src: PiasSourceState<*, *>, typeOfSrc: Type, context: JsonSerializationContext): JsonElement {
        val map = JsonObject()
        map.add(PIAS_KEY, context.serialize(mapOf(Pair(ADDRESS_KEY, src.piasAddress))))
        map.add(RESOLUTION_KEY, context.serialize(src.resolution))
        map.add(OFFSET_KEY, context.serialize(src.offset))
        map.add(DATATYPE_KEY, context.serialize(src.type))
        map.addProperty(NAME_KEY, src.nameProperty().get())
        return map
    }

    override fun getTargetClass(): Class<PiasSourceState<*, *>> {
        return PiasSourceState::class.java
    }

}

class JsonElementIsNotJsonObject(
        val element: JsonElement?,
        message: String? = "Expected ${JsonObject::class.java.name} but got $element") : PainteraException(message)
class RequiredKeyNotFound(
        val key: String,
        val clazz: KClass<*>,
        val map: JsonObject,
        message: String? = "Entry of type $clazz required at key $key but not found in $map") : PainteraException(message)

@Plugin(type = StatefulSerializer.DeserializerFactory::class)
class PiasDeserializerFactory : StatefulSerializer.DeserializerFactory<State, JsonDeserializer<State>> {
    override fun createDeserializer
            (arguments: StatefulSerializer.Arguments,
             projectDirectory: Supplier<String>, dependencyFromIndex:
             IntFunction<SourceState<*, *>>): JsonDeserializer<State> {

        class Deserializer : JsonDeserializer<State> {
            override fun deserialize(json: JsonElement?, typeOfT: Type, context: JsonDeserializationContext): State {
                val map = json?.takeIf { it.isJsonObject }?.asJsonObject ?: throw JsonElementIsNotJsonObject(json)
                val piasMap = map.takeIf { it.has(PIAS_KEY) }?.get(PIAS_KEY)?.takeIf { it.isJsonObject }?.asJsonObject ?: throw RequiredKeyNotFound(PIAS_KEY, JsonObject::class, map)
                val address = piasMap.takeIf { it.has(ADDRESS_KEY) }?.get(ADDRESS_KEY)?.asString ?: throw RequiredKeyNotFound("$PIAS_KEY/$ADDRESS_KEY", String::class, map)
                val resolution = map.takeIf { it.has(RESOLUTION_KEY) }?.get(RESOLUTION_KEY)?.let { context.deserialize(it, DoubleArray::class.java) as DoubleArray } ?: throw RequiredKeyNotFound(RESOLUTION_KEY, DoubleArray::class, map)
                val offset = map.takeIf { it.has(OFFSET_KEY) }?.get(OFFSET_KEY)?.let { context.deserialize(it, DoubleArray::class.java) as DoubleArray } ?: throw RequiredKeyNotFound(OFFSET_KEY, DoubleArray::class, map)
                val name = map.takeIf { it.has(NAME_KEY) }?.get(NAME_KEY)?.asString ?: throw RequiredKeyNotFound(NAME_KEY, String::class, map)
                val type = map.takeIf { it.has(DATATYPE_KEY) }?.get(DATATYPE_KEY)?.let { context.deserialize(it, CompatibleType::class.java) as CompatibleType } ?: throw RequiredKeyNotFound(DATATYPE_KEY, CompatibleType::class, map)

                fun <D, T> create() where D: IntegerType<D>, D: NativeType<D>, T: NativeType<T> = PiasSourceState<D, T>(
                                piasAddress = address,
                                resolution = resolution,
                                offset = offset,
                                name = name,
                                projectDirectory = {projectDirectory.get()},
                                paintera = arguments.viewer);

                return when(type) {
                    CompatibleType.LABEL_MULTISETS -> create<LabelMultisetType, VolatileLabelMultisetType>()
                    CompatibleType.INT64 -> create<LongType, VolatileLongType>()
                    CompatibleType.UINT64 -> create<UnsignedLongType, VolatileUnsignedLongType>()
                    CompatibleType.UINT32 -> create<UnsignedIntType, VolatileUnsignedIntType>()
                }
            }
        }

        return Deserializer()

    }

    override fun getTargetClass(): Class<State> {
        return PiasSourceState::class.java
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())
    }

}