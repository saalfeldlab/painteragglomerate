package org.janelia.saalfeldlab.paintera

import javafx.application.Application

class PainterAgglomerate2 {

    companion object {

        fun addTempDirArg(args: Array<String>): Array<String> {
            return if (args.none { it == "--default-to-temp-directory" })
                args + arrayOf("--default-to-temp-directory") else
                args
        }
    }
}

fun main(args: Array<String>) {
    Application.launch(Paintera::class.java, *PainterAgglomerate2.addTempDirArg(args))
}

