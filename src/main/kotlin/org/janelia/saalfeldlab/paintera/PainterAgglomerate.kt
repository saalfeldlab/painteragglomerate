package org.janelia.saalfeldlab.paintera

import bdv.viewer.ViewerOptions
import javafx.application.Application
import javafx.application.Platform
import javafx.scene.Scene
import javafx.stage.Stage
import picocli.CommandLine
import java.util.*

class PainterAgglomerate : Application() {


    override fun start(primaryStage: Stage?) {


        val args = parameters.getRaw().toTypedArray();

        val cmdLineArgs = CmdLineArgs()
        val parsedSuccessfully = Optional.ofNullable(CommandLine.call(cmdLineArgs, System.err, *args)).orElse(false)

        if (!parsedSuccessfully) {
            return
        }

        Platform.setImplicitExit(true)

        val pbv = PainteraBaseView(1, ViewerOptions.options())
        val scene = Scene(pbv.pane(), 800.0, 600.0)
        primaryStage!!.scene = scene
        primaryStage.show()
    }

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {

            Application.launch(PainterAgglomerate::class.java, *args)

        }
    }

}

