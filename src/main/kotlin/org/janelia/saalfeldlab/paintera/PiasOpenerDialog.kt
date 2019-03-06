package org.janelia.saalfeldlab.paintera

import javafx.beans.property.SimpleObjectProperty
import javafx.beans.property.SimpleStringProperty
import org.janelia.saalfeldlab.fx.ui.Exceptions
import org.janelia.saalfeldlab.fx.ui.ObjectField
import org.janelia.saalfeldlab.n5.N5Writer
import org.janelia.saalfeldlab.paintera.ui.opendialog.menu.OpenDialogMenuEntry
import org.janelia.saalfeldlab.paintera.ui.opendialog.menu.n5.GenericBackendDialogN5
import org.janelia.saalfeldlab.paintera.ui.opendialog.menu.n5.N5OpenSourceDialog
import org.scijava.plugin.Plugin
import org.slf4j.LoggerFactory
import java.lang.invoke.MethodHandles
import java.util.concurrent.ExecutorService
import java.util.function.BiConsumer
import java.util.function.Supplier

class PiasOpenerDialog {

    private val address = SimpleStringProperty(DEFAULT_ADDRESS)

//    private val writerSupplier = SimpleObjectProperty<Supplier<N5Writer>>({ null })

    fun backendDialog(propagationExecutor: ExecutorService): GenericBackendDialogN5 {
        val addressField = ObjectField.stringField(address.get(), ObjectField.SubmitOn.ENTER_PRESSED, ObjectField.SubmitOn.ENTER_PRESSED)
        val containerTextField = addressField.textField()
        addressField.valueProperty().bindBidirectional(address)
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
        }
    }


    companion object {
        private val USER_HOME = System.getProperty("user.home")

        private val H5_EXTENSIONS = arrayOf("*.h5", "*.hdf", "*.hdf5")

        private val DEFAULT_ADDRESS = PainteraConfigYaml.getConfig(Supplier { "ipc:///tmp/pias" }, "data", "pias", "defaultAddress") as String

        private val FAVORITES = PainteraConfigYaml.getConfig(Supplier { mutableListOf<String>() }, "data", "hdf5", "favorites") as List<String>

        private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

    }
}