package org.janelia.saalfeldlab.paintera

import com.sun.javafx.application.PlatformImpl
import javafx.application.Platform
import javafx.beans.property.ReadOnlyBooleanProperty
import javafx.beans.property.SimpleBooleanProperty
import javafx.beans.property.SimpleIntegerProperty
import javafx.beans.property.SimpleStringProperty
import javafx.event.EventHandler
import javafx.scene.control.Alert
import javafx.scene.control.Button
import javafx.scene.control.ButtonType
import javafx.scene.control.Dialog
import javafx.scene.control.MenuButton
import javafx.scene.control.MenuItem
import javafx.scene.control.TextField
import javafx.scene.layout.HBox
import javafx.scene.layout.VBox
import org.janelia.saalfeldlab.fx.ui.Exceptions
import org.janelia.saalfeldlab.fx.util.InvokeOnJavaFXApplicationThread
import org.janelia.saalfeldlab.paintera.control.assignment.FragmentSegmentAssignmentPias
import org.janelia.saalfeldlab.paintera.ui.PainteraAlerts
import org.janelia.saalfeldlab.paintera.util.zmq.sockets.clientSocket
import org.slf4j.LoggerFactory
import org.zeromq.ZMQ
import org.zeromq.ZMQException
import java.lang.invoke.MethodHandles
import java.util.function.Supplier

class SpecificPiasOpener {

    private val address = SimpleStringProperty(DEFAULT_ADDRESS)

    private val pingSuccessful = SimpleBooleanProperty(false)

    fun createDialog(): Dialog<ButtonType> {
        val dialog = Dialog<ButtonType>()
        dialog.title = Paintera.NAME
        dialog.headerText = "Open pias dataset"
        dialog.dialogPane.buttonTypes.setAll(ButtonType.OK, ButtonType.CANCEL)
        (dialog.dialogPane.lookupButton(ButtonType.OK) as Button).let {
            it.text = "_Ok"
            it.disableProperty().bind(pingSuccessful.not())
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

        val enterUrl = MenuItem("_Enter URL")
        enterUrl.setOnAction { EnterUrlDialog(containerTextField.text).let {
                it.showAndWait().let { r -> if (r.filter { ButtonType.OK == it }.isPresent()) { containerTextField.text = null; containerTextField.text = it.urlPrompt.text } } } }
        val connectButton = MenuButton("Co_nnect", null, enterUrl)
        connectButton.isMnemonicParsing = true

        val content = VBox(HBox(containerTextField, connectButton))
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

        var IO_THREADS = (PainteraConfigYaml.getConfig(Supplier { "10" }, "data", "pias", "ioThreads") as String).toInt()

        private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

        private val ALWAYS_FALSE: ReadOnlyBooleanProperty = SimpleBooleanProperty(false)

    }

}

class EnterUrlDialog(val initialString: String?): Dialog<ButtonType>() {

    val urlPrompt = TextField(initialString)

    init {
        urlPrompt.promptText = "URL..."
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