package org.janelia.saalfeldlab.paintera

import javafx.application.Application
import org.slf4j.LoggerFactory
import java.lang.invoke.MethodHandles

class PainterAgglomerate2 {

    companion object {

        fun addTempDirArg(args: Array<String>): Array<String> {
            return if (args.none { it == "--default-to-temp-directory" })
                args + arrayOf("--default-to-temp-directory") else
                args
        }
    }
}

private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

fun main(args: Array<String>) {
    LOG.warn("This is just a helper class for testing! For real application, run plain Paintera and add this jar with dependencies to classpath.")
    Application.launch(Paintera::class.java, *PainterAgglomerate2.addTempDirArg(args))
}

