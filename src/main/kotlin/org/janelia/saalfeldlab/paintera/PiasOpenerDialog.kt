package org.janelia.saalfeldlab.paintera

import bdv.viewer.Source
import com.sun.javafx.application.PlatformImpl
import javafx.application.Platform
import javafx.beans.binding.Bindings
import javafx.beans.property.DoubleProperty
import javafx.beans.property.ReadOnlyBooleanProperty
import javafx.beans.property.SimpleBooleanProperty
import javafx.beans.property.SimpleIntegerProperty
import javafx.beans.property.SimpleObjectProperty
import javafx.beans.property.SimpleStringProperty
import javafx.collections.ListChangeListener
import javafx.event.EventHandler
import javafx.scene.control.Alert
import javafx.scene.control.Button
import javafx.scene.control.ButtonType
import javafx.scene.control.Dialog
import javafx.scene.control.MenuButton
import javafx.scene.control.MenuItem
import javafx.scene.control.TextField
import javafx.scene.input.KeyCode
import javafx.scene.input.KeyEvent
import javafx.scene.layout.HBox
import javafx.scene.layout.Priority
import javafx.scene.layout.VBox
import net.imglib2.Interval
import net.imglib2.realtransform.AffineTransform3D
import net.imglib2.type.Type
import net.imglib2.type.numeric.IntegerType
import net.imglib2.type.numeric.integer.LongType
import net.imglib2.type.numeric.integer.UnsignedIntType
import net.imglib2.type.numeric.integer.UnsignedLongType
import net.imglib2.type.volatiles.VolatileLongType
import net.imglib2.type.volatiles.VolatileUnsignedIntType
import net.imglib2.type.volatiles.VolatileUnsignedLongType
import org.janelia.saalfeldlab.fx.ui.Exceptions
import org.janelia.saalfeldlab.fx.util.InvokeOnJavaFXApplicationThread
import org.janelia.saalfeldlab.n5.DataType
import org.janelia.saalfeldlab.n5.DatasetAttributes
import org.janelia.saalfeldlab.n5.N5Reader
import org.janelia.saalfeldlab.n5.N5Writer
import org.janelia.saalfeldlab.paintera.composition.ARGBCompositeAlphaYCbCr
import org.janelia.saalfeldlab.paintera.control.assignment.FragmentSegmentAssignmentPias
import org.janelia.saalfeldlab.paintera.control.lock.LockedSegmentsOnlyLocal
import org.janelia.saalfeldlab.paintera.control.selection.SelectedIds
import org.janelia.saalfeldlab.paintera.data.DataSource
import org.janelia.saalfeldlab.paintera.data.mask.Masks
import org.janelia.saalfeldlab.paintera.data.n5.CommitCanvasN5
import org.janelia.saalfeldlab.paintera.data.n5.N5FSMeta
import org.janelia.saalfeldlab.paintera.exception.PainteraException
import org.janelia.saalfeldlab.paintera.meshes.InterruptibleFunction
import org.janelia.saalfeldlab.paintera.meshes.MeshManagerWithAssignmentForSegments
import org.janelia.saalfeldlab.paintera.state.LabelSourceState
import org.janelia.saalfeldlab.paintera.stream.HighlightingStreamConverter
import org.janelia.saalfeldlab.paintera.stream.ModalGoldenAngleSaturatedHighlightingARGBStream
import org.janelia.saalfeldlab.paintera.ui.PainteraAlerts
import org.janelia.saalfeldlab.paintera.ui.opendialog.DatasetInfo
import org.janelia.saalfeldlab.paintera.ui.opendialog.menu.OpenDialogMenuEntry
import org.janelia.saalfeldlab.paintera.ui.opendialog.meta.MetaPanel
import org.janelia.saalfeldlab.paintera.util.zmq.sockets.clientSocket
import org.janelia.saalfeldlab.paintera.util.zmq.sockets.toInt
import org.janelia.saalfeldlab.util.MakeUnchecked
import org.janelia.saalfeldlab.util.n5.N5Data
import org.janelia.saalfeldlab.util.n5.N5Helpers
import org.scijava.plugin.Plugin
import org.slf4j.LoggerFactory
import org.zeromq.ZMQ
import org.zeromq.ZMQException
import java.lang.invoke.MethodHandles
import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.Callable
import java.util.function.BiConsumer
import java.util.function.Consumer
import java.util.function.Supplier

class PiasOpenerDialog {

    private val address = SimpleStringProperty(DEFAULT_ADDRESS)

    private val pingSuccessful = SimpleBooleanProperty(false)

    private val n5meta = Bindings.createObjectBinding(Callable { if (pingSuccessful.get()) n5MetaFromPiasNoThrowShowExceptions(address.get(), recvTimeout = 1000, sendTimeout = 1000) else null }, pingSuccessful )

    private val n5 = Bindings.createObjectBinding(Callable { n5meta.get()?.writer() }, n5meta)

    private val dataset = Bindings.createObjectBinding(Callable { n5meta.get()?.dataset()}, n5meta)

    private val isN5Valid = n5.isNotNull

    private val isDatasetValid = dataset.isNotNull

    private val isPainteraDataset = Bindings.createObjectBinding(Callable { isN5Valid.get() && isDatasetValid.get() && N5Helpers.isPainteraDataset(n5.get(), dataset.get()) }, n5 ,dataset, isDatasetValid)

    private val datasetAttributes = Bindings.createObjectBinding(Callable {
        dataset.get()?.let {ds ->
            n5.get()
                    ?.takeIf { isPainteraDataset.get() }
                    ?.let {
                        try {
                            it.getDatasetAttributes("$ds/data/s0")
                        } catch (e: Exception) {
                            LOG.debug("Unable to get attributes from {} in {}", dataset.get(), it)
                            Platform.runLater { Exceptions.exceptionAlert("Unable to update dataset attributes for paintera dataset `$ds' in container `$it'", e).show() }
                            null
                        }
                    }
                    .let{ LOG.debug("Updating dataset attributes to {}", it); it }
        }
    }, dataset, isPainteraDataset)

    private val resolution = Bindings.createObjectBinding(Callable { dataset.get()?.let { d -> n5.get()?.takeIf { isPainteraDataset.get() }?.let { N5Helpers.getResolution(it, d) } } }, n5, dataset, isPainteraDataset)

    private val offset = Bindings.createObjectBinding(Callable { dataset.get()?.let { d -> n5.get().takeIf { isPainteraDataset.get() }?.let { N5Helpers.getOffset(it, d) } } }, n5, dataset, isPainteraDataset)

    private val dimensions = SimpleObjectProperty<LongArray?>(longArrayOf(-1, -1, -1))

    private val datasetInfo = DatasetInfo()

    private val dataType = Bindings.createObjectBinding(Callable {
        dataset.get()?.let{ ds ->
            n5.get()
                    ?.takeIf { isPainteraDataset.get() }
                    ?.let { datasetAttributes.get()?.let { attrs -> try {
                        getDataType(it, ds, attrs)
                    } catch (e: Exception) {
                        Platform.runLater { Exceptions.exceptionAlert("Unable to update dataset type for paintera dataset `$ds' in container `$it'", e).show() }
                        null
                    } } }
        }
    }, datasetAttributes)

    private val isValid = isN5Valid.and(isDatasetValid).and(datasetAttributes.isNotNull).and(dataType.isNotNull)

    private val metaPanel = MetaPanel()

    init {
        resolution.addListener { _, _, newv -> newv?.let{ it.forEachIndexed { dim, res -> datasetInfo.spatialResolutionProperties()[dim].set(res) } } }
        offset.addListener { _, _, newv -> newv?.let{ it.forEachIndexed { dim, off -> datasetInfo.spatialOffsetProperties()[dim].set(off) } } }
        datasetAttributes.addListener { _, _, newv ->
            LOG.debug("New attributes: {}", newv); dimensions.value = newv?.dimensions.let { LOG.info("Updating dimensions {}", it); it } }
        dimensions.addListener { _, _, newv -> LOG.info("Updated dimensions to {}", newv)}
    }

    private fun createDialog(): Dialog<ButtonType> {
        val dialog = Dialog<ButtonType>()
        dialog.title = Paintera.NAME
        dialog.headerText = "Open pias dataset"
        dialog.dialogPane.buttonTypes.setAll(ButtonType.OK, ButtonType.CANCEL)
        (dialog.dialogPane.lookupButton(ButtonType.OK) as Button).let {
            it.text = "_Ok"
            it.disableProperty().bind(isValid.not())
        }
        (dialog.dialogPane.lookupButton(ButtonType.CANCEL) as Button).let { it.text = "_Cancel" }

        val containerTextField = TextField("")
        containerTextField.isEditable = false
        containerTextField.textProperty().bindBidirectional(address)
        containerTextField.textProperty().set(address.get())
        containerTextField.textProperty().addListener { _, _, newv -> pingSuccessful.bind(newv?.let { pingServerAndWait(it) }?: ALWAYS_FALSE)}
        containerTextField.minWidth = 0.0
        containerTextField.maxWidth = java.lang.Double.POSITIVE_INFINITY
        containerTextField.promptText = "PIAS address"

        val menuPrompt = MenuItem("_Enter URL")
        menuPrompt.setOnAction { UrlPromptDialog(containerTextField.text).let {
                it.showAndWait().let { r -> if (r.filter { ButtonType.OK == it }.isPresent) { containerTextField.text = null; containerTextField.text = it.urlPrompt.text } } } }
        // _C is already reserved for _Cancel
        // for some reason, C_onnect does not work
        val connectButton = MenuButton("Co_nnect", null, menuPrompt)

        metaPanel.listenOnDimensions(dimensions)
        datasetInfo.spatialOffsetProperties().let { metaPanel.listenOnOffset(it[0], it[1], it[2]) }
        datasetInfo.spatialResolutionProperties().let { metaPanel.listenOnResolution(it[0], it[1], it[2]) }
        metaPanel.revertButton.onAction = EventHandler { datasetInfo.spatialResolutionProperties().revertValues(); datasetInfo.spatialOffsetProperties().revertValues() }

        HBox.setHgrow(containerTextField, Priority.ALWAYS)
        val content = VBox(HBox(containerTextField, connectButton), metaPanel.pane)
        dialog.dialogPane.content = content
        LOG.info("Returning dialog with content {}", content)

        return dialog
    }

    fun showDialogAndAddIfOk(paintera: PainteraBaseView, projectDirectory: () -> String?) {
        val dialog = createDialog()
        if (dialog.showAndWait().filter { ButtonType.OK.equals(it) }.isPresent) {
            paintera.addPiasSource(
                    dataType.value!!,
                    n5.value!!,
                    dataset.value!!,
                    datasetInfo.spatialResolutionProperties().map { it.value }.toDoubleArray(),
                    datasetInfo.spatialOffsetProperties().map { it.value }.toDoubleArray(),
                    "PIAS",
                    ioThreads = IO_THREADS,
                    projectDirectory = projectDirectory,
                    piasAddress = address.value)
        }
    }

    fun pingServerAndWait(address: String): ReadOnlyBooleanProperty {
        val context = ZMQ.context(IO_THREADS)
        val pingAddress = FragmentSegmentAssignmentPias.pingAddress(address)
        val onError: (ZMQException) -> ZMQ.Socket? = { it ->
            context.term()
            LOG.info("Invalid address {}", address)
            Exceptions.exceptionAlert("Invalid address $address", it).showAndWait()
            null }
        val timeout = 100
        return try { clientSocket(context, pingAddress, receiveTimeout = timeout) } catch (e: ZMQException) { onError(e) }?.let { socket ->
            val dialog = PainteraAlerts.alert(Alert.AlertType.CONFIRMATION)
            val baseString = "Waiting for response from server at `${address}' ..."
            dialog.headerText = baseString
            val totalWaitingTime = SimpleIntegerProperty(0)
            totalWaitingTime.addListener { _, _, newv -> InvokeOnJavaFXApplicationThread.invoke { dialog.headerText = "$baseString   ${totalWaitingTime.get().toDouble() / 1000.0}s" } }
            val pingSuccessful = SimpleBooleanProperty(false)
            LOG.info("Pinging server at address $pingAddress")
            var wasCanceled = false
            dialog.dialogPane.lookupButton(ButtonType.OK).disableProperty().bind(pingSuccessful.not())
            val t = Thread {

                try {

                    while (!pingSuccessful.get() && !wasCanceled) {
                        try {
                            socket.send("")
                            socket.recv()?.run {
                                InvokeOnJavaFXApplicationThread.invoke { dialog.headerText = "Reached server after ${(totalWaitingTime.get()).toDouble() / 1000.0}s" }
                                pingSuccessful.set(true)
                            }
                        } catch (e: ZMQException) {
                            Thread.sleep(timeout.toLong())
                        } finally {
                            totalWaitingTime.set(totalWaitingTime.get() + timeout)
                        }
                        if (!wasCanceled && totalWaitingTime.get() > 1000 && !dialog.isShowing)
                            InvokeOnJavaFXApplicationThread.invoke { dialog.show() }
                    }
                }
                finally {
                    context.term()
                }
            }
            dialog.onHidden = EventHandler { wasCanceled = true }
            t.start()
            pingSuccessful
        } ?: ALWAYS_FALSE.let { LOG.info("Returning always false"); it }
    }


    companion object {
        private val DEFAULT_ADDRESS = PainteraConfigYaml.getConfig(Supplier { null }, "data", "pias", "defaultAddress") as String?

        var IO_THREADS = (PainteraConfigYaml.getConfig(Supplier { "1" }, "data", "pias", "ioThreads") as String).toInt()

        private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

        private val ALWAYS_FALSE: ReadOnlyBooleanProperty = SimpleBooleanProperty(false)

        private val N5_META_ENDPOINT = "/api/n5/all"

        @Throws(InvalidDataType::class)
        private fun getDataType(reader: N5Reader, dataset: String, attributes: DatasetAttributes): DataTypeOrLabelMultisetType {
            return attributes.dataType.let {
                try {
                    DataTypeOrLabelMultisetType(it, false)
                } catch (e: InvalidDataType) {
                    if (it == DataType.UINT8 && reader.getAttribute("$dataset/data", N5Helpers.IS_LABEL_MULTISET_KEY, Boolean::class.java) == true) {
                        DataTypeOrLabelMultisetType(it, true)
                    } else
                        throw e
                }
            }
        }

        private fun Array<out DoubleProperty>.revertValues() {
            (0 until size / 2).forEach {
                val tmp = this[it].get()
                this[it].set(this[size - 1 - it].get())
                this[size - 1 - it].set(tmp)
            }
        }

        fun n5MetaFromPiasNoThrowShowExceptions(address: String, recvTimeout: Int = -1, sendTimeout: Int = -1): N5FSMeta? {
            return try {
                LOG.info("Trying to get n5 meta from pias at address {}", address)
                val meta = n5MetaFromPias(address, recvTimeout, sendTimeout)
                LOG.info("Got n5 meta {} from pias at address {}", meta, address)
                meta
            } catch (e: Exception) {
                Exceptions.exceptionAlert("Unable to retrieve data set information from PIAS at $address within ${recvTimeout}ms", e)
                null
            }
        }

        @Throws(PiasEndpointException::class)
        private fun n5MetaFromPias(address: String, recvTimeout: Int = - 1, sendTimeout: Int = -1): N5FSMeta?  {
            val context = ZMQ.context(IO_THREADS)
            return try {
                val socket = try {
                    clientSocket(context, address, receiveTimeout = recvTimeout, sendTimeout = sendTimeout)
                } catch (e: Exception) {
                    LOG.error("Unable to get n5 meta information from PIAS server at {}: {}", address, e.message)
                    null
                }
                socket?.let {
                    it.send(N5_META_ENDPOINT)
                    // returnCode success: 0
                    val returnCode = it.recv().toInt()
                    val success = returnCode == 0
                    val numMessages = it.recv().toInt()
                    if (success)
                        require(numMessages == 2) { "Did expect 2 messages but got $numMessages" }
                    val n5metaList = mutableListOf<String>()
                    val messages = (0 until numMessages).map {index ->
                        val messageType = it.recv().toInt()
                        if (success) {
                            // messageType == 0: string
                            require(messageType == 0) { "Message type $messageType not consistent with expected type 0"}
                            val msg = it.recvStr(Charset.defaultCharset())
                            n5metaList.add(msg)
                            msg
                        } else
                            it.recv()
                    }
                    it.close()
                    if (!success)
                        throw PiasEndpointException(address, returnCode, *messages.toTypedArray(), "Request to $address/$N5_META_ENDPOINT returned non-successfully")
                    N5FSMeta(n5metaList[0], n5metaList[1])
                }

            } finally {
                context.term()
            }
        }

        private fun AffineTransform3D.fromResolutionAndOffset(
                resolution: DoubleArray = DoubleArray(numDimensions()) {1.0},
                offset: DoubleArray = DoubleArray(numDimensions(), {0.0} )): AffineTransform3D {
            set(
                    resolution[0], 0.0, 0.0, offset[0],
                    0.0, resolution[1], 0.0, offset[1],
                    0.0, 0.0, resolution[2], offset[2]
            )
            return this
        }

        @Throws(InvalidDataType::class)
        private fun PainteraBaseView.addPiasSource(
                dataType: DataTypeOrLabelMultisetType,
                container: N5Writer,
                dataset: String,
                resolution: DoubleArray,
                offset: DoubleArray,
                name: String,
                piasAddress: String,
                projectDirectory: () -> String?,
                ioThreads: Int = IO_THREADS
        ) {
            val tf = AffineTransform3D().fromResolutionAndOffset(resolution = resolution, offset = offset)
            val cache = globalCache
            if (dataType.isLabelMultiset) {
                addPiasSource(N5Data.openLabelMultisetAsSource(container, dataset, tf, globalCache, 0, name), container, dataset, piasAddress, projectDirectory, ioThreads)
            } else {

                when (dataType.dataType) {
                    DataType.UINT32 -> addPiasSource(N5Data.openScalarAsSource<UnsignedIntType, VolatileUnsignedIntType>(container, dataset, tf, cache, 0, name), container, dataset, piasAddress, projectDirectory, ioThreads)
                    DataType.UINT64 -> addPiasSource(N5Data.openScalarAsSource<UnsignedLongType, VolatileUnsignedLongType>(container, dataset, tf, cache, 0, name), container, dataset, piasAddress, projectDirectory, ioThreads)
                    DataType.INT64 -> addPiasSource(N5Data.openScalarAsSource<LongType, VolatileLongType>(container, dataset, tf, cache, 0, name), container, dataset, piasAddress, projectDirectory, ioThreads)
                    else -> throw DataTypeOrLabelMultisetType.exceptionFor(dataType.dataType)
                }
            }

        }

        private fun <D: IntegerType<D>, T: Type<T>> PainteraBaseView.addPiasSource(
                source: DataSource<D, T>,
                container: N5Writer,
                dataset: String,
                piasAddress: String,
                projectDirectory: () -> String?,
                ioThreads: Int = IO_THREADS
        ) {
            val context = ZMQ.context(ioThreads)
            val idService = N5Helpers.idService(container, dataset)
            val selectedIds = SelectedIds()
            val assignment = FragmentSegmentAssignmentPias(piasAddress = piasAddress, context = context, idService = idService)
            val lockedSegments = LockedSegmentsOnlyLocal(Consumer{})
            val stream = ModalGoldenAngleSaturatedHighlightingARGBStream(selectedIds, assignment, lockedSegments)
            val converter = HighlightingStreamConverter.forType(stream, source.type)

            val nextCanvas = Supplier { ( projectDirectory()?.let { Paths.get(it, "canvases").let { it.toFile().mkdirs(); Files.createTempDirectory(it, "canvas-") } } ?: Files.createTempDirectory("canvas-")).toAbsolutePath().toString()}
            // TODO use CommitCanvas that updates edge features!!!
            val maskedSource = Masks.mask(source, null, nextCanvas, CommitCanvasN5(container, dataset), this.propagationQueue)


            val lookup = N5Helpers.getLabelBlockLookup(container, dataset)

            val loaderForLevelFactory = { level: Int ->
                InterruptibleFunction.fromFunction(
                        MakeUnchecked.function<Long, Array<Interval>>(
                                { id -> lookup.read(level, id!!) },
                                { id -> LOG.debug("Falling back to empty array"); arrayOf() }))
            }

            val blockLoaders = (0 until maskedSource.numMipmapLevels).map { loaderForLevelFactory(it) }.toTypedArray()

            val meshManager = MeshManagerWithAssignmentForSegments.fromBlockLookup<D>(
                    maskedSource,
                    selectedIds,
                    assignment,
                    stream,
                    viewer3D().meshesGroup(),
                    blockLoaders,
                    { globalCache.createNewCache(it) },
                    meshManagerExecutorService,
                    meshWorkerExecutorService)


            val state: LabelSourceState<D, T>? = LabelSourceState(
                    maskedSource,
                    converter,
                    ARGBCompositeAlphaYCbCr(),
                    source.name,
                    assignment,
                    lockedSegments,
                    idService,
                    selectedIds,
                    meshManager,
                    lookup)
            ListChangeListener<Source<*>> { while (it.next()) { if (it.removed.contains(source)) { assignment.close(); context.term(); sourceInfo() } } }.let {
                this.sourceInfo().removedSourcesTracker().addListener(it)
            }
            this.addLabelSource(state)
        }

    }

    @Plugin(type = OpenDialogMenuEntry::class, menuPath = "_Pias", priority = java.lang.Double.MAX_VALUE)
    class MenuEntry : OpenDialogMenuEntry {

        override fun onAction(): BiConsumer<PainteraBaseView, String> {
            return BiConsumer{ pbv, projectDirectory ->
                try {
                    LOG.info("Creating and showing dialog")
                    PiasOpenerDialog().showDialogAndAddIfOk(pbv, {projectDirectory})
                } catch (e1: Exception) {
                    LOG.debug("Unable to open pias dataset", e1)
                    Exceptions.exceptionAlert(Paintera.NAME, "Unable to open pias data set", e1).show()
                }
            }
        }

        companion object {
            private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())
        }
    }

}

class PiasEndpointException(
        val address: String,
        val returnCode: Int,
        vararg val messages: Any,
        val exceptionMessage: String? = null): PainteraException(exceptionMessage)

class InvalidDataType(val actual: DataType, vararg val choices: DataType, message: String = "Found data type $actual but require one of ${choices}") : PainteraException(message)

class UrlPromptDialog(val initialString: String?): Dialog<ButtonType>() {

    val urlPrompt = TextField(initialString)

    init {
        urlPrompt.promptText = "URL..."
        // need to catch enter because JVM segfaults when hitting enter inside textfield - WTF!
        // https://stackoverflow.com/questions/18512654/jvm-crashes-on-pressing-press-enter-key-in-a-textfield
        urlPrompt.addEventHandler(KeyEvent.KEY_PRESSED) { it.code?.takeIf { KeyCode.ENTER.equals(it) }?.run { it.consume() } }
    }

    init {
        title = Paintera.NAME
        headerText = "Enter URL to connect to PIAS"
        dialogPane.content = urlPrompt
        dialogPane.buttonTypes.setAll(ButtonType.OK, ButtonType.CANCEL)
        (dialogPane.lookupButton(ButtonType.OK) as Button).text = "_Ok"
        (dialogPane.lookupButton(ButtonType.CANCEL) as Button).text = "_Cancel"
        onShowing = EventHandler { Platform.runLater { urlPrompt.requestFocus() } }
    }
}


data class DataTypeOrLabelMultisetType @Throws(InvalidDataType::class) constructor(val dataType: DataType, val isLabelMultiset: Boolean) {

    init {
        dataType.takeIf { validNonMultisetTypesAsSet.contains(dataType) || isLabelMultiset } ?: throw exceptionFor(dataType)
    }

    companion object {
        val validNonMultisetTypes = arrayOf(DataType.UINT32, DataType.UINT64, DataType.INT64)
        val validNonMultisetTypesAsSet = validNonMultisetTypes.toSet()
        fun exceptionFor(dataType: DataType) : InvalidDataType {
            return InvalidDataType(
                    dataType,
                    *validNonMultisetTypes,
                    message = "Found data type $dataType but require one of ${validNonMultisetTypesAsSet} or tag `\"isLabelMultiset\":true' in attributes.json")
        }
    }

}

fun main(args: Array<String>) {
    PlatformImpl.startup {}
    val opener = PiasOpenerDialog()
    Platform.setImplicitExit(true)
//    Platform.runLater { opener.createDialog().showAndWait() }
}