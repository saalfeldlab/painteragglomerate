package org.janelia.saalfeldlab.paintera

import com.sun.javafx.application.PlatformImpl
import javafx.application.Platform
import javafx.beans.binding.Bindings
import javafx.beans.property.DoubleProperty
import javafx.beans.property.ReadOnlyBooleanProperty
import javafx.beans.property.SimpleBooleanProperty
import javafx.beans.property.SimpleIntegerProperty
import javafx.beans.property.SimpleObjectProperty
import javafx.beans.property.SimpleStringProperty
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
import org.janelia.saalfeldlab.fx.ui.Exceptions
import org.janelia.saalfeldlab.fx.util.InvokeOnJavaFXApplicationThread
import org.janelia.saalfeldlab.paintera.control.assignment.FragmentSegmentAssignmentPias
import org.janelia.saalfeldlab.paintera.data.n5.N5FSMeta
import org.janelia.saalfeldlab.paintera.exception.PainteraException
import org.janelia.saalfeldlab.paintera.ui.PainteraAlerts
import org.janelia.saalfeldlab.paintera.ui.opendialog.DatasetInfo
import org.janelia.saalfeldlab.paintera.ui.opendialog.menu.OpenDialogMenuEntry
import org.janelia.saalfeldlab.paintera.ui.opendialog.meta.MetaPanel
import org.janelia.saalfeldlab.paintera.util.zmq.sockets.clientSocket
import org.janelia.saalfeldlab.paintera.util.zmq.sockets.toInt
import org.janelia.saalfeldlab.util.n5.N5Helpers
import org.scijava.plugin.Plugin
import org.slf4j.LoggerFactory
import org.zeromq.ZMQ
import org.zeromq.ZMQException
import java.lang.invoke.MethodHandles
import java.nio.charset.Charset
import java.util.concurrent.Callable
import java.util.function.BiConsumer
import java.util.function.Supplier

class SpecificPiasOpener {

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
    }, n5, dataset, isPainteraDataset)

    private val resolution = Bindings.createObjectBinding(Callable { dataset.get()?.let { d -> n5.get()?.takeIf { isPainteraDataset.get() }?.let { N5Helpers.getResolution(it, d) } } }, n5, dataset, isPainteraDataset)

    private val offset = Bindings.createObjectBinding(Callable { dataset.get()?.let { d -> n5.get().takeIf { isPainteraDataset.get() }?.let { N5Helpers.getOffset(it, d) } } }, n5, dataset, isPainteraDataset)

    private val dimensions = SimpleObjectProperty<LongArray?>(longArrayOf(-1, -1, -1))

    private val datasetInfo = DatasetInfo()

    private val isValid = isN5Valid.and(isDatasetValid).and(datasetAttributes.isNotNull)

    private val metaPanel = MetaPanel()

    init {
        resolution.addListener { _, _, newv -> newv?.let{ it.forEachIndexed { dim, res -> datasetInfo.spatialResolutionProperties()[dim].set(res) } } }
        offset.addListener { _, _, newv -> newv?.let{ it.forEachIndexed { dim, off -> datasetInfo.spatialOffsetProperties()[dim].set(off) } } }
        datasetAttributes.addListener { _, _, newv ->
            LOG.debug("New attributes: {}", newv); dimensions.value = newv?.dimensions.let { LOG.info("Updating dimensions {}", it); it } }
        dimensions.addListener { _, _, newv -> LOG.info("Updated dimensions to {}", newv)}
    }

    fun createDialog(): Dialog<ButtonType> {
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

    }

    @Plugin(type = OpenDialogMenuEntry::class, menuPath = "_Pias", priority = java.lang.Double.MAX_VALUE)
    class MenuEntry : OpenDialogMenuEntry {

        override fun onAction(): BiConsumer<PainteraBaseView, String> {
            return BiConsumer{ pbv, projectDirectory ->
                try {
                    LOG.info("Creating and showing dialog")
                    SpecificPiasOpener().createDialog().showAndWait()
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

fun main(args: Array<String>) {
    PlatformImpl.startup {}
    val opener = SpecificPiasOpener()
    Platform.setImplicitExit(true)
    Platform.runLater { opener.createDialog().showAndWait() }
}