package org.janelia.saalfeldlab.paintera

import javafx.application.Application
import javafx.application.Platform
import javafx.scene.Scene
import javafx.scene.input.MouseEvent
import javafx.stage.Stage
import net.imglib2.Volatile
import net.imglib2.converter.ARGBColorConverter
import net.imglib2.type.NativeType
import net.imglib2.type.numeric.ARGBType
import net.imglib2.type.numeric.IntegerType
import net.imglib2.type.numeric.RealType
import org.janelia.saalfeldlab.paintera.composition.ARGBCompositeAlphaYCbCr
import org.janelia.saalfeldlab.paintera.composition.CompositeCopy
import org.janelia.saalfeldlab.paintera.control.assignment.FragmentSegmentAssignmentState
import org.janelia.saalfeldlab.paintera.control.assignment.ServerClientFragmentSegmentAssignment
import org.janelia.saalfeldlab.paintera.control.assignment.ZMQAssignmentActionBroadcasterActionsHandler
import org.janelia.saalfeldlab.paintera.control.lock.LockedSegmentsOnlyLocal
import org.janelia.saalfeldlab.paintera.control.selection.SelectedIds
import org.janelia.saalfeldlab.paintera.data.DataSource
import org.janelia.saalfeldlab.paintera.data.mask.Masks
import org.janelia.saalfeldlab.paintera.data.n5.CommitCanvasN5
import org.janelia.saalfeldlab.paintera.id.IdSelectorZMQ
import org.janelia.saalfeldlab.paintera.meshes.cache.BlocksForLabelFromFile
import org.janelia.saalfeldlab.paintera.solver.CurrentSolutionZMQ
import org.janelia.saalfeldlab.paintera.state.LabelSourceState
import org.janelia.saalfeldlab.paintera.state.RawSourceState
import org.janelia.saalfeldlab.paintera.stream.HighlightingStreamConverter
import org.janelia.saalfeldlab.paintera.stream.ModalGoldenAngleSaturatedHighlightingARGBStream
import org.slf4j.LoggerFactory
import org.zeromq.ZMQ
import picocli.CommandLine
import java.lang.invoke.MethodHandles
import java.util.*
import java.util.function.Consumer
import java.util.regex.Pattern

class PainterAgglomerate : Application() {

    private val viewer = PainteraBaseView.defaultView()
    private val pbv = viewer.baseView
    private val context = ZMQ.context(1)

    override fun start(primaryStage: Stage) {


        try {
            val args = parameters.raw.toTypedArray()

            val cmdLineArgs = CmdLineArgs()
            val parsedSuccessfully = Optional.ofNullable(CommandLine.call(cmdLineArgs, System.err, *args)).orElse(false)

            if (!parsedSuccessfully) {
                return
            }

            val projectDirectory = cmdLineArgs.project() ?: createTempDir("painteragglomerate").absolutePath

            Platform.setImplicitExit(true)

            // TODO("Define these addresses")
            val solverServerAddress = "ipc:///tmp/mc-solver"
            val solutionDistributionAddress = "ipc:///tmp/current-solution"

            val idSelector = IdSelectorZMQ(solverServerAddress, context)

            val assignment = ServerClientFragmentSegmentAssignment(ZMQAssignmentActionBroadcasterActionsHandler(context, solverServerAddress))
            val solutionFetcher = CurrentSolutionZMQ(context, solverServerAddress, solutionDistributionAddress, "solution", 500, assignment)
            assignment.accept(solutionFetcher.currentSolution())

            cmdLineArgs.rawSources.forEachIndexed({ index, rs -> Paintera.addRawFromStringNoGenerics(pbv, rs, if (index == 0) CompositeCopy<ARGBType>() else ARGBCompositeAlphaYCbCr()) })

            cmdLineArgs.labelSources.forEach({
                Paintera.addLabelFromStringNoGenerics(
                        pbv,
                        it,
                        projectDirectory,
                        { _, _ -> assignment },
                        { _, _ -> idSelector })
            })

            val scene = Scene(viewer.paneWithStatus.pane, 800.0, 600.0)

            viewer.keyTracker.installInto(scene)
            scene.addEventFilter(MouseEvent.ANY, viewer.mouseTracker)
            Platform.setImplicitExit(true)

            primaryStage.scene = scene
            primaryStage.show()
        } catch (e: Exception)
        {
            LOG.error("Caught exception in start-up!", e)
        }
    }

    override fun stop() {
        LOG.warn("Stopping application")
        pbv.stop()
        context.close()
    }

    companion object {

        private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

        @JvmStatic
        fun main(args: Array<String>) {

            Application.launch(PainterAgglomerate::class.java, *args)

        }

        @Throws(UnableToAddSource::class)
        private fun <D, T> addRawFromString(
                pbv: PainteraBaseView,
                identifier: String): Optional<DataSource<D, T>> where D : RealType<D>, D : NativeType<D>, T : NativeType<T>, T : RealType<T>, T : Volatile<D> {
            if (!Pattern.matches("^[a-z]+://.+", identifier)) {
                return addRawFromString(pbv, "file://$identifier")
            }

            if (Pattern.matches("^file://.+", identifier)) {
                try {
                    val split = identifier.replaceFirst("file://".toRegex(), "").split(":".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
                    val reader = N5Helpers.n5Reader(split[0], 64, 64, 64)
                    val dataset = split[1]
                    val name = N5Helpers.lastSegmentOfDatasetPath(dataset)

                    val source = N5Helpers.openRawAsSource<D, T, Any>(
                            reader,
                            dataset,
                            N5Helpers.getTransform(reader, dataset),
                            pbv.queue,
                            0,
                            name)

                    val state = RawSourceState<D, T>(
                            source,
                            ARGBColorConverter.Imp1<T>(),
                            CompositeCopy<ARGBType>(),
                            name)

                    pbv.addRawSource(state)
                    return Optional.of(state.dataSource)
                } catch (e: Exception) {
                    throw e as? UnableToAddSource ?: UnableToAddSource(e)
                }

            }

            LOG.warn("Unable to generate raw source from {}", identifier)
            return Optional.empty()
        }

        @Throws(UnableToAddSource::class)
        private fun <D, T> addLabelFromString(
                pbv: PainteraBaseView,
                identifier: String,
                projectDirectory: String,
                assignment: FragmentSegmentAssignmentState
        ) where D : NativeType<D>, D : IntegerType<D>, T : NativeType<T> {
            if (!Pattern.matches("^[a-z]+://.+", identifier)) {
                addLabelFromString<D, T>(pbv, "file://$identifier", projectDirectory, assignment)
                return
            }

            if (Pattern.matches("^file://.+", identifier)) {
                try {
                    val split = identifier.replaceFirst("file://".toRegex(), "").split(":".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
                    val n5 = N5Helpers.n5Writer(split[0], 64, 64, 64)
                    val dataset = split[1]
                    LOG.warn("Adding label dataset={} dataset={}", split[0], dataset)
                    val resolution = N5Helpers.getResolution(n5, dataset)
                    val offset = N5Helpers.getOffset(n5, dataset)
                    val transform = N5Helpers.fromResolutionAndOffset(resolution, offset)
                    val nextCanvasDir = Masks.canvasTmpDirDirectorySupplier(projectDirectory)
                    val name = N5Helpers.lastSegmentOfDatasetPath(dataset)
                    val selectedIds = SelectedIds()
                    val idService = N5Helpers.idService(n5, dataset)
                    val lockedSegments = LockedSegmentsOnlyLocal(Consumer { _ -> })
                    val stream = ModalGoldenAngleSaturatedHighlightingARGBStream(
                            selectedIds,
                            assignment,
                            lockedSegments)
                    val dataSource = N5Helpers.openAsLabelSource<D, T>(
                            n5,
                            dataset,
                            transform,
                            pbv.queue,
                            0,
                            name)

                    val maskedSource = Masks.mask(
                            dataSource,
                            nextCanvasDir.get(),
                            nextCanvasDir,
                            CommitCanvasN5(n5, dataset),
                            pbv.propagationQueue)

                    val blockLoaders = Arrays
                            .stream(N5Helpers.labelMappingFromFileLoaderPattern(n5, dataset))
                            .map<BlocksForLabelFromFile>({ BlocksForLabelFromFile(it) })
                            .toArray<BlocksForLabelFromFile>({ n: Int -> Array<BlocksForLabelFromFile?>(n, { _ -> null }) })

                    val state = LabelSourceState<D, T>(
                            maskedSource,
                            HighlightingStreamConverter.forType(stream, dataSource.type),
                            ARGBCompositeAlphaYCbCr(),
                            name,
                            assignment,
                            lockedSegments,
                            idService,
                            selectedIds,
                            pbv.viewer3D().meshesGroup(),
                            blockLoaders,
                            pbv.meshManagerExecutorService,
                            pbv.meshWorkerExecutorService)
                    pbv.addLabelSource(state)
                } catch (e: Exception) {
                    throw e as? UnableToAddSource ?: UnableToAddSource(e)
                }

            }
        }

    }

}

