package org.janelia.saalfeldlab.paintera

import org.slf4j.LoggerFactory
import picocli.CommandLine
import java.io.File
import java.lang.invoke.MethodHandles
import java.util.*
import java.util.concurrent.Callable

@CommandLine.Command(name = "PainterAgglomerate")
class CmdLineArgs : Callable<Boolean> {

    @CommandLine.Option(names = ["-h", "--help"], usageHelp = true, description = ["Display this help message."])
    private var helpRequested: Boolean = false

    @CommandLine.Option(names = ["--raw-source"], paramLabel = "RAW_SOURCE", required = false, description = ["Open raw source at start-up. Has to be [file://]/path/to/<n5-or-hdf5>:path/to/dataset"])
    var rawSources: Array<String> = arrayOf()
        private set

    @CommandLine.Option(names = ["--label-source"], paramLabel = "LABEL_SOURCE", required = false, description = ["Open label source at start-up. Has to be [file://]/path/to/<n5-or-hdf5>:path/to/dataset"])
    var labelSources: Array<String> = arrayOf()
        private set

    @CommandLine.Option(names = ["--num-screen-scales"], paramLabel = "NUM_SCREEN_SCALES", required = false, description = ["Number of screen scales, defaults to 3"])
    private var numScreenScales: Int? = null

    @CommandLine.Option(names = ["--highest-screen-scale"], paramLabel = "HIGHEST_SCREEN_SCALE", required = false, description = ["Highest screen scale, restricted to the interval (0,1], defaults to 1"])
    private var highestScreenScale: Double? = null

    @CommandLine.Option(names = ["--screen-scale-factor"], paramLabel = "SCREEN_SCALE_FACTOR", required = false, description = ["Scalar value from the open interval (0,1) that defines how screen scales diminish in each dimension. Defaults to 0.5"])
    private var screenScaleFactor: Double? = null

    @CommandLine.Option(names = ["--screen-scales"], paramLabel = "SCREEN_SCALES", required = false, description = ["Explicitly set screen scales. Must be strictliy monotonically decreasing values in from the interval (0,1]. Overrides all other screen scale options."], arity = "1..*", split = ",")
    private var screenScales: DoubleArray? = null

    @CommandLine.Parameters(index = "0", paramLabel = "PROJECT", arity = "0..1", description = ["Optional project N5 root (N5 or HDF5)."])
    private var project: String? = null


    @Throws(Exception::class)
    override fun call(): Boolean? {
        numScreenScales = Optional.ofNullable(this.numScreenScales).filter { n -> n > 0 }.orElse(3)
        highestScreenScale = Optional.ofNullable(highestScreenScale).filter { s -> s > 0 && s <= 1 }.orElse(1.0)
        screenScaleFactor = Optional.ofNullable(screenScaleFactor).filter { f -> f > 0 && f < 1 }.orElse(0.5)
        screenScales = if (screenScales == null) createScreenScales(numScreenScales!!, highestScreenScale!!, screenScaleFactor!!) else screenScales

        if (screenScales != null) {
            checkScreenScales(screenScales!!)
        }

        return true
    }

    fun project(): String? {
        val returnedProject = if (this.project == null) this.project else File(project).absolutePath
        LOG.debug("Return project={}", returnedProject)
        return returnedProject
    }

    fun screenScales(): DoubleArray {
        return this.screenScales!!.clone()
    }

    class ZeroLengthScreenScales : Exception()

    class InvalidScreenScaleValue internal constructor(scale: Double) : Exception("Screen scale $scale not in legal interval (0,1]")

    class ScreenScaleNotDecreasing(first: Double, second: Double) : Exception("Second screen scale $second larger than or equal to first $first")

    companion object {

        private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

        @Throws(ZeroLengthScreenScales::class)
        private fun createScreenScales(numScreenScales: Int, highestScreenScale: Double, screenScaleFactor: Double): DoubleArray {
            if (numScreenScales <= 1) {
                throw ZeroLengthScreenScales()
            }

            val screenScales = DoubleArray(numScreenScales)
            screenScales[0] = highestScreenScale
            for (i in 1 until screenScales.size) {
                screenScales[i] = screenScaleFactor * screenScales[i - 1]
            }
            LOG.debug("Returning screen scales {}", screenScales)
            return screenScales
        }

        @Throws(ZeroLengthScreenScales::class, InvalidScreenScaleValue::class, ScreenScaleNotDecreasing::class)
        private fun checkScreenScales(screenScales: DoubleArray) {
            if (screenScales.isEmpty()) {
                throw ZeroLengthScreenScales()
            }

            if (screenScales[0] <= 0 || screenScales[0] > 1) {
                throw InvalidScreenScaleValue(screenScales[0])
            }

            for (i in 1 until screenScales.size) {
                val prev = screenScales[i - 1]
                val curr = screenScales[i]
                // no check for > 1 necessary because already checked for
                // monotonicity
                if (curr <= 0) {
                    throw InvalidScreenScaleValue(curr)
                }
                if (prev <= curr) {
                    throw ScreenScaleNotDecreasing(prev, curr)
                }
            }

        }
    }

}
