package org.janelia.saalfeldlab.paintera.solver

import com.google.gson.*
import org.janelia.saalfeldlab.paintera.control.assignment.action.AssignmentAction
import java.lang.reflect.Type

class AssignmentActionJsonAdapter : JsonSerializer< AssignmentAction >, JsonDeserializer<AssignmentAction> {


    companion object {
        private val TYPE_KEY = "type";

        private val DATA_KEY = "data";
    }
    override fun deserialize(json: JsonElement, typeOfT: Type, context: JsonDeserializationContext): AssignmentAction {
        val obj = json.asJsonObject
        val type : AssignmentAction.Type  = context.deserialize(obj.get(TYPE_KEY), AssignmentAction.Type::class.java)
        return context.deserialize(obj.get(DATA_KEY), type.classForType )
    }

    override fun serialize(src: AssignmentAction, typeOfSrc: Type, context: JsonSerializationContext): JsonElement {
        val obj = JsonObject()
        obj.add(TYPE_KEY, context.serialize(src.type))
        obj.add(DATA_KEY, context.serialize(obj))
        return obj
    }
}