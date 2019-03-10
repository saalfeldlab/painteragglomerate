package org.janelia.saalfeldlab.paintera

import bdv.viewer.Interpolation
import javafx.beans.property.ObjectProperty
import javafx.beans.property.SimpleBooleanProperty
import javafx.beans.property.SimpleObjectProperty
import javafx.beans.property.SimpleStringProperty
import javafx.beans.value.ObservableBooleanValue
import javafx.event.Event
import javafx.event.EventHandler
import javafx.scene.input.KeyCode
import javafx.scene.input.KeyEvent
import net.imglib2.Interval
import net.imglib2.realtransform.AffineTransform3D
import net.imglib2.type.NativeType
import net.imglib2.type.numeric.ARGBType
import net.imglib2.type.numeric.IntegerType
import org.janelia.saalfeldlab.fx.event.DelegateEventHandlers
import org.janelia.saalfeldlab.fx.event.EventFX
import org.janelia.saalfeldlab.fx.event.KeyTracker
import org.janelia.saalfeldlab.fx.ui.Exceptions
import org.janelia.saalfeldlab.labels.blocks.LabelBlockLookup
import org.janelia.saalfeldlab.paintera.composition.ARGBCompositeAlphaYCbCr
import org.janelia.saalfeldlab.paintera.composition.Composite
import org.janelia.saalfeldlab.paintera.control.assignment.FragmentSegmentAssignmentPias
import org.janelia.saalfeldlab.paintera.control.lock.LockedSegmentsOnlyLocal
import org.janelia.saalfeldlab.paintera.control.selection.SelectedIds
import org.janelia.saalfeldlab.paintera.data.DataSource
import org.janelia.saalfeldlab.paintera.data.axisorder.AxisOrder
import org.janelia.saalfeldlab.paintera.data.n5.N5FSMeta
import org.janelia.saalfeldlab.paintera.exception.PainteraException
import org.janelia.saalfeldlab.paintera.id.IdService
import org.janelia.saalfeldlab.paintera.meshes.InterruptibleFunction
import org.janelia.saalfeldlab.paintera.meshes.MeshManagerWithAssignmentForSegments
import org.janelia.saalfeldlab.paintera.state.LabelSourceStateIdSelectorHandler
import org.janelia.saalfeldlab.paintera.state.LabelSourceStateMergeDetachHandler
import org.janelia.saalfeldlab.paintera.state.LabelSourceStatePaintHandler
import org.janelia.saalfeldlab.paintera.state.SourceState
import org.janelia.saalfeldlab.paintera.stream.HighlightingStreamConverter
import org.janelia.saalfeldlab.paintera.stream.ModalGoldenAngleSaturatedHighlightingARGBStream
import org.janelia.saalfeldlab.util.MakeUnchecked
import org.janelia.saalfeldlab.util.n5.N5Data
import org.janelia.saalfeldlab.util.n5.N5Helpers
import org.slf4j.LoggerFactory
import org.zeromq.ZMQ
import java.io.Closeable
import java.lang.invoke.MethodHandles
import java.util.function.Consumer

class PiasSourceState<D, T> @Throws(DidNotReachPias::class) constructor(
        val piasAddress: String,
        name: String,
        val resolution: DoubleArray,
        val offset: DoubleArray,
        paintera: PainteraBaseView,
        val projectDirectory: () -> String?,
        ioThreads: Int = defaultIoThreads,
        val recvTimeoutMillis: Int = 500,
        val sendTimeoutMillis: Int = 500):
        SourceState<D, T>, Closeable  where D : IntegerType<D>, D: NativeType<D>, T: NativeType<T> {

    val context = ZMQ.context(ioThreads)

    private val ping = PiasPing(context, piasAddress, recvTimeoutMillis, sendTimeoutMillis)


    val meta: N5FSMeta
    val source: DataSource<D, T>
    val selectedIds: SelectedIds = SelectedIds()
    val lockedSegments = LockedSegmentsOnlyLocal(Consumer {})
    val idService: IdService
    val assignment: FragmentSegmentAssignmentPias
    val stream: ModalGoldenAngleSaturatedHighlightingARGBStream
    val converter: HighlightingStreamConverter<T>
    val lookup: LabelBlockLookup
    val meshManager: MeshManagerWithAssignmentForSegments

    private val composite: ObjectProperty<Composite<ARGBType, ARGBType>> = SimpleObjectProperty(ARGBCompositeAlphaYCbCr())
    private val interpolation = SimpleObjectProperty(Interpolation.NEARESTNEIGHBOR)
    private val name = SimpleStringProperty()
    private val axisOrder = SimpleObjectProperty(AxisOrder.XYZ)
    private val isVisible = SimpleBooleanProperty(true)
    private val isDirty = SimpleBooleanProperty(true)


    private val paintHandler: LabelSourceStatePaintHandler = LabelSourceStatePaintHandler(selectedIds)
    private val idSelectorHandler: LabelSourceStateIdSelectorHandler
    private val mergeDetachHandler: LabelSourceStateMergeDetachHandler

    init {

        try {
            this.ping.pingSuccesfulProperty().addListener { _, _, newv ->
                if (newv)
                    LOG.info("Ping successful!")
                else
                    LOG.error("Unable to ping server in {} seconds", (ping.lastSuccessfulPingProperty().value - System.nanoTime()) * 1e-9)
            }
            this.meta = PiasContainer.n5MetaFromPias(context, piasAddress, recvTimeoutMillis, sendTimeoutMillis)!!
            val unmaskedSource = N5Data.openAsLabelSource<D, T>(
                    meta.writer(),
                    meta.dataset(),
                    AffineTransform3D().fromResolutionAndOffset(resolution, offset),
                    paintera.globalCache,
                    0,
                    name)
            // currently no masked source supported, TODO add later
            this.source = unmaskedSource
//            this.source = Masks.mask(unmaskedSource, null, )
            this.idService = N5Helpers.idService(meta.writer(), meta.dataset())
            this.assignment = FragmentSegmentAssignmentPias(piasAddress, idService, context)
            this.stream = ModalGoldenAngleSaturatedHighlightingARGBStream(selectedIds, assignment, lockedSegments)
            this.converter = HighlightingStreamConverter.forType(stream, source.type)!!
            this.name.value = name

            this.idSelectorHandler = LabelSourceStateIdSelectorHandler(this.source, selectedIds, assignment, lockedSegments)
            this.mergeDetachHandler = LabelSourceStateMergeDetachHandler(dataSource, selectedIds, assignment, idService);

            this.lookup = N5Helpers.getLabelBlockLookup(meta.reader(), meta.dataset())

            val loaderForLevelFactory = { level: Int ->
                InterruptibleFunction.fromFunction(
                        MakeUnchecked.function<Long, Array<Interval>>(
                                { id -> LOG.debug("Looking up id {} at level {}", id, level); lookup.read(level, id!!) },
                                { id -> LOG.debug("Falling back to empty array"); arrayOf() }))
            }

            val blockLoaders = (0 until this.source.numMipmapLevels).map { loaderForLevelFactory(it) }.toTypedArray()

            this.meshManager = MeshManagerWithAssignmentForSegments.fromBlockLookup<D>(
                    this.source,
                    selectedIds,
                    assignment,
                    stream,
                    paintera.viewer3D().meshesGroup(),
                    blockLoaders,
                    { paintera.globalCache.createNewCache(it) },
                    paintera.meshManagerExecutorService,
                    paintera.meshWorkerExecutorService)

            this.assignment.updateSolution(recvTimeout = recvTimeoutMillis, sendTimeout = sendTimeoutMillis)


        } catch (e: Exception) {
            Exceptions.exceptionAlert("Unable to create pias source", e)
            close()
            throw e
        }
    }

    override fun onAdd(paintera: PainteraBaseView) {
        converter().getStream().addListener { obs -> paintera.orthogonalViews().requestRepaint() }
        selectedIds.addListener { obs -> paintera.orthogonalViews().requestRepaint() }
        lockedSegments.addListener { obs -> paintera.orthogonalViews().requestRepaint() }
        meshManager.areMeshesEnabledProperty().bind(paintera.viewer3D().isMeshesEnabledProperty)
        assignment.addListener { obs -> paintera.orthogonalViews().requestRepaint() }
    }

    override fun converter(): HighlightingStreamConverter<T> = converter

    override fun getDataSource(): DataSource<D, T> = source

    override fun compositeProperty(): ObjectProperty<Composite<ARGBType, ARGBType>> = composite
    override fun dependsOn(): Array<SourceState<*, *>> = arrayOf()
    override fun interpolationProperty() = interpolation
    override fun nameProperty() = name
    override fun axisOrderProperty() = axisOrder
    override fun isVisibleProperty() = isVisible

    private fun invalidateAll() {
        this.meshManager.invalidateMeshCaches()
    }

    fun refreshMeshes() {
        this.invalidateAll()
        val selection = this.selectedIds.activeIds
        val lastSelection = this.selectedIds.lastSelection
        this.selectedIds.deactivateAll()
        this.selectedIds.activate(*selection)
        this.selectedIds.activateAlso(lastSelection)
    }

    override fun close() {
        ping.close()
        context.term()
        assignment.close()
    }

    // those probably need implementation:
    // also, these methods are probably obsolete (paintera will always ask if you would like to save project)
    override fun clean() = LOG.warn("${this::class.simpleName}#clean not implemented")

    override fun stain() = LOG.warn("${this::class.simpleName}#stain not implemented")
    override fun isDirty() = isDirtyProperty.get()
    override fun isDirtyProperty() = isDirty as ObservableBooleanValue

    override fun stateSpecificViewerEventHandler(paintera: PainteraBaseView, keyTracker: KeyTracker): EventHandler<Event> {
        LOG.info("Returning {}-specific handler", javaClass.simpleName)
        val handler = DelegateEventHandlers.listHandler<Event>()
        // TODO add paint support
//        handler.addHandler(paintHandler.viewerHandler(paintera, keyTracker))
        handler.addHandler(idSelectorHandler.viewerHandler(paintera, keyTracker))
        handler.addHandler(mergeDetachHandler.viewerHandler(paintera, keyTracker))
        return handler
    }

    override fun stateSpecificGlobalEventHandler(paintera: PainteraBaseView, keyTracker: KeyTracker): EventHandler<Event> {
        LOG.debug("Returning {}-specific global handler", javaClass.simpleName)
        val handler = DelegateEventHandlers.handleAny()
        handler.addEventHandler(
                KeyEvent.KEY_PRESSED,
                EventFX.KEY_PRESSED("refresh meshes", { e ->
                    LOG.debug("Key event triggered refresh meshes")
                    refreshMeshes()
                }, { e -> keyTracker.areOnlyTheseKeysDown(KeyCode.R) }))
        return handler
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

        var defaultIoThreads = 3

        private fun AffineTransform3D.fromResolutionAndOffset(
                resolution: DoubleArray = DoubleArray(numDimensions()) { 1.0 },
                offset: DoubleArray = DoubleArray(numDimensions()) { 0.0 }): AffineTransform3D {
            set(
                    resolution[0], 0.0, 0.0, offset[0],
                    0.0, resolution[1], 0.0, offset[1],
                    0.0, 0.0, resolution[2], offset[2]
            )
            return this
        }

    }

    class DidNotReachPias(
            val address: String,
            val timeInSeconds: Double,
            message: String = "Unable to reach server at $address within ${timeInSeconds} seconds") : PainteraException(message)
}
