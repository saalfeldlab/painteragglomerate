package org.janelia.saalfeldlab.paintera

import javafx.application.Application
import net.imglib2.FinalInterval
import net.imglib2.algorithm.neighborhood.DiamondShape
import net.imglib2.img.array.ArrayImgs
import net.imglib2.interpolation.randomaccess.NearestNeighborInterpolator
import net.imglib2.interpolation.randomaccess.NearestNeighborInterpolatorFactory
import net.imglib2.realtransform.RealViews
import net.imglib2.realtransform.Scale3D
import net.imglib2.realtransform.ScaleAndTranslation
import net.imglib2.type.numeric.integer.UnsignedLongType
import net.imglib2.util.Intervals
import net.imglib2.view.Views
import org.janelia.saalfeldlab.n5.GzipCompression
import org.janelia.saalfeldlab.n5.N5FSWriter
import org.janelia.saalfeldlab.n5.RawCompression
import org.janelia.saalfeldlab.n5.imglib2.N5Utils
import org.janelia.saalfeldlab.paintera.control.assignment.LabeledEdge
import picocli.CommandLine
import java.io.File
import java.nio.file.Files
import java.util.concurrent.Callable
import kotlin.math.max
import kotlin.math.min


class Args : Callable<Unit> {

    @CommandLine.Option(names = arrayOf("--container"), paramLabel = "CONTAINER", defaultValue = defaultContainer)
    var containerPath: String = defaultContainer

    @CommandLine.Option(names = arrayOf("--dataset"), paramLabel = "DATASET", defaultValue = defaultPainteraDataset)
    var dataset: String = defaultPainteraDataset

    override fun call() {
        val dims = longArrayOf(2, 3, 2)
        val data = ArrayImgs.unsignedLongs(LongArray(12) { it + 1L }, *dims)
        val expandedData = Views.interval(Views.raster(RealViews.transformReal(
                Views.interpolate(Views.extendBorder(data), NearestNeighborInterpolatorFactory()),
                ScaleAndTranslation(doubleArrayOf(40.0, 40.0, 40.0), doubleArrayOf(20.0, 20.0, 20.0)))),
                FinalInterval(*dims.map { it * 40 }.toLongArray()))
        val resolution = doubleArrayOf(10.0, 10.0, 10.0)
        val labeledEdges = arrayOf(
                LabeledEdge(1, 2,  1),
                LabeledEdge(1, 3, 1),
                LabeledEdge(1, 7, 0),
                LabeledEdge(2, 4, 1),
                LabeledEdge(2, 8, 0),
                LabeledEdge(3, 4, 1),
                LabeledEdge( 3, 5, 0),
                LabeledEdge(3, 9, 0),
                LabeledEdge(4, 6, 0),
                LabeledEdge(4, 10, 0),
                LabeledEdge(5, 6, 1),
                LabeledEdge(5, 11, 1),
                LabeledEdge(6, 12, 1),
                LabeledEdge(7, 8, 1),
                LabeledEdge(7, 9, 1),
                LabeledEdge(8, 10, 1),
                LabeledEdge(9, 10, 1),
                LabeledEdge(9, 11, 1),
                LabeledEdge(10, 12, 1),
                LabeledEdge(11, 12, 1))
        val edges = ArrayImgs.unsignedLongs(labeledEdges.map { arrayOf(it.edge.e1, it.edge.e2) }.toTypedArray().flatten().toLongArray(), 2L, labeledEdges.size.toLong())
        val edge_features = ArrayImgs.doubles(labeledEdges.map { it.label.toDouble() }.toDoubleArray(), 1L, labeledEdges.size.toLong())
        val edgeMap = mutableMapOf<Long,  MutableMap<Long, Int>>()
        labeledEdges.forEach { edgeMap.computeIfAbsent(it.edge.e1) { mutableMapOf()}[it.edge.e2] = it.label }

        val affinities = ArrayImgs.floats(*Intervals.dimensionsAsLongArray(expandedData))
        Views.interval(Views.pair(Views.pair(affinities, expandedData), DiamondShape(1).neighborhoodsRandomAccessible(Views.extendBorder(expandedData))), affinities).forEach {
            val a = it.a.a
            val cp = it.a.b
            val e1 = cp.integerLong
            val nh = it.b
            var isEdge = false
            for (np in nh) {
                    val e2 = np.integerLong
                    isEdge = edgeMap[min(e1, e2)]?.get(max(e1, e2)) == 0
                if (isEdge)
                    break
            }
            a.setReal(if (isEdge)  0.0 else 1.0)

        }


        val container = N5FSWriter(containerPath)
        N5Utils.save(expandedData, container, "$dataset/data/s0", intArrayOf(32, 32, 32), RawCompression())
        N5Utils.save(affinities, container, "affinities", intArrayOf(32, 32, 32), RawCompression())
        N5Utils.save(edges, container, "$dataset/edges", Intervals.dimensionsAsIntArray(edges), RawCompression())
        N5Utils.save(edge_features, container, "$dataset/edge-features", Intervals.dimensionsAsIntArray(edge_features), RawCompression())

        container.setAttribute(dataset, "painteraData", mapOf(Pair("type", "label")))
        container.setAttribute(dataset, "maxId", 12)
        container.setAttribute("$dataset/data", "resolution", resolution)
        container.setAttribute("affinities", "resolution", resolution)

        val piasCmd = "pias --container $containerPath --paintera-dataset=$dataset --address='ipc:///tmp/pias' --log-level=TRACE"
//        val process = try {Runtime.getRuntime().exec(piasCmd)} catch(e: Exception) {null}
        println(piasCmd)

        Application.launch(Paintera::class.java, *PainterAgglomerate2.addTempDirArg(arrayOf()))

    }

    private fun containerInTmp(): String {
        val dir = Files.createTempDirectory("dummy-piac-data")
        return dir.resolve("container.n5").toAbsolutePath().toString()
    }

    companion object {
        const val defaultPainteraDataset = "paintera-dataset"
        const val defaultContainer = "/data/hanslovskyp/piac-dummy-data.n5"
    }

}

fun main(argv: Array<String>) {
    CommandLine.call(Args(), *argv)
}