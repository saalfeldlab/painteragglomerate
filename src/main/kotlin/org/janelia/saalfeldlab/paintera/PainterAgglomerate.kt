package org.janelia.saalfeldlab.paintera

import javafx.application.Application
import javafx.application.Platform
import javafx.scene.Scene
import javafx.scene.input.MouseEvent
import javafx.stage.Stage
import net.imglib2.Interval
import net.imglib2.Volatile
import net.imglib2.converter.ARGBColorConverter
import net.imglib2.type.NativeType
import net.imglib2.type.label.LabelMultisetType
import net.imglib2.type.label.VolatileLabelMultisetType
import net.imglib2.type.numeric.ARGBType
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
import org.janelia.saalfeldlab.paintera.data.n5.N5DataSource
import org.janelia.saalfeldlab.paintera.data.n5.N5Meta
import org.janelia.saalfeldlab.paintera.id.IdSelectorZMQ
import org.janelia.saalfeldlab.paintera.id.IdService
import org.janelia.saalfeldlab.paintera.meshes.InterruptibleFunction
import org.janelia.saalfeldlab.paintera.meshes.MeshManagerWithAssignmentForSegments
import org.janelia.saalfeldlab.paintera.solver.CurrentSolutionZMQ
import org.janelia.saalfeldlab.paintera.state.LabelSourceState
import org.janelia.saalfeldlab.paintera.state.RawSourceState
import org.janelia.saalfeldlab.paintera.stream.HighlightingStreamConverter
import org.janelia.saalfeldlab.paintera.stream.ModalGoldenAngleSaturatedHighlightingARGBStream
import org.janelia.saalfeldlab.util.MakeUnchecked
import org.janelia.saalfeldlab.util.n5.N5Helpers
import org.slf4j.LoggerFactory
import org.zeromq.ZMQ
import picocli.CommandLine
import java.lang.invoke.MethodHandles
import java.util.*
import java.util.function.Consumer
import java.util.regex.Pattern
import java.util.stream.Collectors
import java.util.stream.IntStream

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

//            cmdLineArgs.rawSources.forEachIndexed({ index, rs -> Paintera.addRawFromStringNoGenerics(pbv, rs, if (index == 0) CompositeCopy<ARGBType>() else ARGBCompositeAlphaYCbCr()) })

            cmdLineArgs.labelSources.forEach({
                addLabelFromString(
                        pbv,
                        it,
                        projectDirectory,
                        assignment,
                        idSelector)
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

                    val source = N5DataSource<D, T>(
                            N5Meta.fromReader(reader, dataset),
                            N5Helpers.getTransform(reader, dataset),
                            pbv.globalCache,
                            name,
                            0)

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
        private fun addLabelFromString(
                pbv: PainteraBaseView,
                identifier: String,
                projectDirectory: String,
                assignment: FragmentSegmentAssignmentState,
                idService : IdService
        ) {
            if (!Pattern.matches("^[a-z]+://.+", identifier)) {
                addLabelFromString(pbv, "file://$identifier", projectDirectory, assignment, idService)
                return
            }

            if (Pattern.matches("^(file|n5|h5)://.+", identifier)) {
                try {
                    val split = identifier.replaceFirst("(file|n5|h5)://".toRegex(), "").split(":".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
                    val root = split[0]
                    val n5 = N5Helpers.n5Writer(root, 64, 64, 64)
                    val group = split[1]
                    if (N5Helpers.isHDF(root))
                        throw IllegalArgumentException("Can only handle N5FS!")
                    val isPainteraDataset = N5Helpers.isPainteraDataset(n5, group)
                    val dataset = if (isPainteraDataset) "$group/data" else group
                    val isMultiScale = N5Helpers.isMultiScale(n5, dataset)
                    val isLabelMultisetType = N5Helpers.getBooleanAttribute(n5, if (isMultiScale) dataset + "/s0" else dataset, N5Helpers.IS_LABEL_MULTISET_KEY, false)
                    if (!isLabelMultisetType)
                        throw IllegalArgumentException("Can only handle LabelMultisetType!")
                    LOG.warn("Adding label dataset={} dataset={}", split[0], group)
                    val resolution = N5Helpers.getResolution(n5, group)
                    val offset = N5Helpers.getOffset(n5, group)
                    val transform = N5Helpers.fromResolutionAndOffset(resolution, offset)
                    val nextCanvasDir = Masks.canvasTmpDirDirectorySupplier(projectDirectory)
                    val name = N5Helpers.lastSegmentOfDatasetPath(group)
                    val selectedIds = SelectedIds()
                    val lockedSegments = LockedSegmentsOnlyLocal(Consumer { _ -> })
                    val stream = ModalGoldenAngleSaturatedHighlightingARGBStream(
                            selectedIds,
                            assignment,
                            lockedSegments)
                    val dataSource = N5DataSource<LabelMultisetType, VolatileLabelMultisetType>(
                            N5Meta.fromReader(n5, dataset),
                            transform,
                            pbv.globalCache,
                            name,
                            0)

                    val maskedSource = Masks.mask(
                            dataSource,
                            nextCanvasDir.get(),
                            nextCanvasDir,
                            CommitCanvasN5(n5, dataset),
                            pbv.propagationQueue)

                    val lookup = N5Helpers.getLabelBlockLookup(n5, group)
                    val blockLoadersFactory = { level: Int ->
                        InterruptibleFunction.fromFunction(
                                MakeUnchecked.function<Long, Array<Interval>>(
                                        { lookup.read(level, it!!) },
                                        { LOG.debug("Falling back to empty array");arrayOf()}
                                ));
                    }

                    val blockLookup = IntStream
                            .range(0, dataSource.numMipmapLevels)
                            .mapToObj(blockLoadersFactory)
                            .collect(Collectors.toList())
                            .toTypedArray()

                    val meshManager = MeshManagerWithAssignmentForSegments.fromBlockLookup(
                            maskedSource,
                            selectedIds,
                            assignment,
                            stream,
                            pbv.viewer3D().meshesGroup(),
                            blockLookup,
                            pbv.globalCache::createNewCache,
                            pbv.meshManagerExecutorService,
                            pbv.meshWorkerExecutorService
                            )

                    val state = LabelSourceState<LabelMultisetType, VolatileLabelMultisetType>(
                            maskedSource,
                            HighlightingStreamConverter.forType(stream, dataSource.type),
                            ARGBCompositeAlphaYCbCr(),
                            name,
                            assignment,
                            lockedSegments,
                            idService,
                            selectedIds,
                            meshManager,
                            lookup)
                    pbv.addLabelSource(state)
                } catch (e: Exception) {
                    throw e as? UnableToAddSource ?: UnableToAddSource(e)
                }

            }
        }

    }

}

