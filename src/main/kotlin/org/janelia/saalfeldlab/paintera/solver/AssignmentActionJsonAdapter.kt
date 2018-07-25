package org.janelia.saalfeldlab.paintera.solver

import com.google.gson.*
import org.janelia.saalfeldlab.paintera.control.assignment.action.AssignmentAction
import org.slf4j.LoggerFactory
import java.lang.invoke.MethodHandles
import java.lang.reflect.Type

class AssignmentActionJsonAdapter : JsonSerializer<AssignmentAction>, JsonDeserializer<AssignmentAction> {


    companion object {
        private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

        private val TYPE_KEY = "type";

        private val DATA_KEY = "data";
    }

    override fun deserialize(json: JsonElement, typeOfT: Type, context: JsonDeserializationContext): AssignmentAction {
        LOG.warn("Deserializing {}", json)
        val obj = json.asJsonObject
        val type: AssignmentAction.Type = context.deserialize(obj.get(TYPE_KEY), AssignmentAction.Type::class.java)
        val deserialized: AssignmentAction = context.deserialize(obj.get(DATA_KEY), type.classForType)
        LOG.warn("Deserialized {}", deserialized)
        return deserialized
    }

    override fun serialize(src: AssignmentAction, typeOfSrc: Type, context: JsonSerializationContext): JsonElement {
        LOG.debug("Serializing {}", src)
        val obj = JsonObject()
        obj.add(TYPE_KEY, context.serialize(src.type))
        val serialized = context.serialize(src)
        obj.add(DATA_KEY, serialized)
        LOG.debug("Serialized {}", obj)
        return obj
    }
}