package org.janelia.saalfeldlab.paintera

import javafx.beans.property.*
import javafx.event.EventHandler
import javafx.scene.control.Alert
import javafx.scene.control.ButtonType
import org.janelia.saalfeldlab.fx.ui.Exceptions
import org.janelia.saalfeldlab.fx.ui.ObjectField
import org.janelia.saalfeldlab.fx.util.InvokeOnJavaFXApplicationThread
import org.janelia.saalfeldlab.n5.N5Writer
import org.janelia.saalfeldlab.paintera.control.assignment.FragmentSegmentAssignmentPias
import org.janelia.saalfeldlab.paintera.ui.PainteraAlerts
import org.janelia.saalfeldlab.paintera.ui.opendialog.menu.OpenDialogMenuEntry
import org.janelia.saalfeldlab.paintera.ui.opendialog.menu.n5.GenericBackendDialogN5
import org.janelia.saalfeldlab.paintera.ui.opendialog.menu.n5.N5OpenSourceDialog
import org.janelia.saalfeldlab.paintera.util.zmq.sockets.clientSocket
import org.scijava.plugin.Plugin
import org.slf4j.LoggerFactory
import org.zeromq.ZMQ
import org.zeromq.ZMQException
import java.lang.invoke.MethodHandles
import java.util.concurrent.ExecutorService
import java.util.function.BiConsumer
import java.util.function.Supplier

class PiasOpenerDialog {

    private val address = SimpleStringProperty(DEFAULT_ADDRESS)

//    private val writerSupplier = SimpleObjectProperty<Supplier<N5Writer>>({ null })

    fun backendDialog(propagationExecutor: ExecutorService): GenericBackendDialogN5 {
        val addressField = ObjectField.stringField("", ObjectField.SubmitOn.ENTER_PRESSED, ObjectField.SubmitOn.ENTER_PRESSED)
        val containerTextField = addressField.textField()
        addressField.valueProperty().addListener { _, _, newv -> newv?.let { PiasOpener.pingServerAndWait(it) } }
        addressField.valueProperty().bindBidirectional(address)
        addressField.valueProperty().set(address.get())
        containerTextField.minWidth = 0.0
        containerTextField.maxWidth = java.lang.Double.POSITIVE_INFINITY
        containerTextField.promptText = "PIAS address"

        val d = GenericBackendDialogN5(containerTextField, javafx.scene.control.Label(""), "PIAS", SimpleObjectProperty(Supplier { null as N5Writer? }), propagationExecutor)
        return d
    }

    fun containerAccepted() {}

    @Plugin(type = OpenDialogMenuEntry::class, menuPath = "_Pias", priority = java.lang.Double.MAX_VALUE)
    class PiasOpener : OpenDialogMenuEntry {

        override fun onAction(): BiConsumer<PainteraBaseView, String> {
            return BiConsumer{ pbv, projectDirectory ->
                try {
                    pias.backendDialog(pbv.getPropagationQueue()).use { dialog ->
                        val osDialog = N5OpenSourceDialog(pbv, dialog)
                        dialog.channelInformation.bindTo(osDialog.meta.channelInformation())
                        osDialog.setHeaderFromBackendType("Pias")
                        val backend = osDialog.showAndWait()
                        if (backend == null || !backend.isPresent)
                        else {
                            N5OpenSourceDialog.addSource(osDialog.name, osDialog.type, dialog, pbv, projectDirectory)
                            pias.containerAccepted()
                        }
                    }
                } catch (e1: Exception) {
                    LOG.debug("Unable to open n5 dataset", e1)
                    Exceptions.exceptionAlert(Paintera.NAME, "Unable to open N5 data set", e1).show()
                }
            }
        }

        companion object {
            private val pias = PiasOpenerDialog()

            fun pingServerAndWait(address: String): ReadOnlyBooleanProperty {
                val context = ZMQ.context(IO_THREADS)
                val dialog = PainteraAlerts.alert(Alert.AlertType.CONFIRMATION)
                val baseString = "Waiting for response from server at `${address}' ..."
                dialog.headerText = baseString
                val totalWaitingTime = SimpleIntegerProperty(0)
                totalWaitingTime.addListener { _, _, newv -> InvokeOnJavaFXApplicationThread.invoke { dialog.headerText = "$baseString   ${totalWaitingTime.get().toDouble() / 1000.0}s" } }
                val timeout = 100

                val pingSuccessful = SimpleBooleanProperty(false)
                val pingAddress = FragmentSegmentAssignmentPias.pingAddress(address)
                LOG.info("Pinging server at address $pingAddress")
                var wasCanceled = false
                dialog.dialogPane.lookupButton(ButtonType.OK).disableProperty().bind(pingSuccessful.not())
                val t = Thread {

                    val socket = clientSocket(context, pingAddress, receiveTimeout = timeout)
                    while (!pingSuccessful.get() && !wasCanceled) {
                        try {
                            socket.send("")
                            socket.recv()?.run{
                                InvokeOnJavaFXApplicationThread.invoke { dialog.headerText = "Reached server after ${(totalWaitingTime.get()).toDouble() / 1000.0}s" }
                                pingSuccessful.set(true)
                            }
                        } catch (e: ZMQException) {
                            Thread.sleep(timeout.toLong())
                        } finally {
                            totalWaitingTime.set(totalWaitingTime.get() + timeout)
                        }
                        if (!wasCanceled && totalWaitingTime.get() > 1000 && !dialog.isShowing)
                            InvokeOnJavaFXApplicationThread.invoke {dialog.show()}
                    }
                }
                dialog.onHidden = EventHandler { wasCanceled = true }
                t.start()
                return pingSuccessful
            }
        }
    }


    companion object {
        private val USER_HOME = System.getProperty("user.home")

        private val H5_EXTENSIONS = arrayOf("*.h5", "*.hdf", "*.hdf5")

        private val DEFAULT_ADDRESS = PainteraConfigYaml.getConfig(Supplier { "ipc:///tmp/pias" }, "data", "pias", "defaultAddress") as String

        var IO_THREADS = (PainteraConfigYaml.getConfig(Supplier { "10" }, "data", "pias", "ioThreads") as String).toInt()

        private val FAVORITES = PainteraConfigYaml.getConfig(Supplier { mutableListOf<String>() }, "data", "hdf5", "favorites") as List<String>

        private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

    }
}