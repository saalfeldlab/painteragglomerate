package org.janelia.saalfeldlab.paintera

import org.janelia.saalfeldlab.fx.ui.Exceptions
import org.janelia.saalfeldlab.paintera.ui.opendialog.menu.OpenDialogMenuEntry
import org.scijava.plugin.Plugin
import org.slf4j.LoggerFactory
import java.lang.invoke.MethodHandles
import java.util.function.BiConsumer

@Plugin(type = OpenDialogMenuEntry::class, menuPath = "_Pias", priority = java.lang.Double.MAX_VALUE)
class PiasOpener : OpenDialogMenuEntry {

    val dialog = SpecificPiasOpener()

    override fun onAction(): BiConsumer<PainteraBaseView, String> {
        return BiConsumer{ pbv, projectDirectory ->
            try {
                LOG.info("Creating and showing dialog")
                dialog.createDialog().showAndWait()
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
