package org.janelia.saalfeldlab.paintera

import javafx.scene.Node
import javafx.scene.control.Alert
import javafx.scene.control.Label
import javafx.scene.control.TextField
import javafx.scene.layout.GridPane
import javafx.scene.layout.Priority
import javafx.scene.layout.VBox
import org.janelia.saalfeldlab.fx.Buttons
import org.janelia.saalfeldlab.fx.TitledPanes
import org.janelia.saalfeldlab.paintera.control.selection.SelectedSegments
import org.janelia.saalfeldlab.paintera.meshes.MeshInfos
import org.janelia.saalfeldlab.paintera.ui.BindUnbindAndNodeSupplier
import org.janelia.saalfeldlab.paintera.ui.PainteraAlerts
import org.janelia.saalfeldlab.paintera.ui.source.mesh.MeshPane
import org.janelia.saalfeldlab.paintera.ui.source.state.SourceStateUIElementsDefaultFactory
import org.scijava.plugin.Plugin
import org.slf4j.LoggerFactory
import java.lang.invoke.MethodHandles
import java.util.function.Supplier

private typealias Factory = SourceStateUIElementsDefaultFactory.AdditionalBindUnbindSuppliersFactory<PiasSourceState<*, *>>

@Plugin(type = SourceStateUIElementsDefaultFactory.AdditionalBindUnbindSuppliersFactory::class)
class LabelSourceStateAdditionalBindAndUnbindSupplierFactory : Factory {

    override fun create(state: PiasSourceState<*, *>): Array<BindUnbindAndNodeSupplier> {
        return arrayOf(piasPane(state), meshPane(state))
    }

    override fun getTargetClass(): Class<PiasSourceState<*, *>> {
        return PiasSourceState::class.java
    }

    companion object {

        private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

        private fun piasPane(state: PiasSourceState<*, *>): BindUnbindAndNodeSupplier {
            val address = state.piasAddress
            val container = state.meta.basePath()
            val dataset = state.meta.dataset()

            val nodeSupplier = Supplier<Node> {
                val grid = GridPane()
                val addressLabel = Label("Address").let { it.prefWidth = 100.0; it }
                val containerLabel = Label("Container").let { it.prefWidth = 100.0; it }
                val datasetLabel = Label("Dataset").let { it.prefWidth = 100.0; it }
                val addressField = TextField(address).let { it.isEditable = false; GridPane.setHgrow(it, Priority.ALWAYS); it }
                val containerField = TextField(container).let { it.isEditable = false; GridPane.setHgrow(it, Priority.ALWAYS); it }
                val datasetField = TextField(dataset).let { it.isEditable = false; GridPane.setHgrow(it, Priority.ALWAYS); it }
                grid.add(addressLabel, 0, 0)
                grid.add(addressField, 1, 0)
                grid.add(containerLabel, 0, 1)
                grid.add(containerField, 1, 1)
                grid.add(datasetLabel, 0, 2)
                grid.add(datasetField, 1, 2)

                val saveGroundTruthButton = Buttons.withTooltip(
                        "Save edge labels",
                        "Persist ground truth edge labels. " +
                                "Persisted labels will be available at next start of PIAS. " +
                                "Previously saved state will be backed up.") {
                    val success = state.assignment.persistGroundTruthLabels(
                            recvTimeout = state.recvTimeoutMillis,
                            sendTimeout = state.sendTimeoutMillis)
                    val dialog = if (success.first && success.second is Int)
                        PainteraAlerts.alert(Alert.AlertType.INFORMATION).let {
                            it.headerText = "Persisted ground truth labels"
                            it.contentText = if (success.second == 1) "No(t enough) labels provided yet" else "Success"
                            it }
                    else
                        PainteraAlerts.alert(Alert.AlertType.ERROR).let {
                            it.headerText = "Unable to persist ground truth labels"
                            it.contentText = "${success.second}"
                            it }
                    dialog.show()
                }
                saveGroundTruthButton.maxWidth = Double.MAX_VALUE

                TitledPanes.createCollapsed("PIAS", VBox(grid, saveGroundTruthButton))
            }
            return BindUnbindAndNodeSupplier.noBind(nodeSupplier)
        }

        private fun meshPane(state: PiasSourceState<*, *>): BindUnbindAndNodeSupplier {
            val assignment = state.assignment
            val selectedIds = state.selectedIds
            val selectedSegments = SelectedSegments(selectedIds, assignment)
            val meshManager = state.meshManager
            val meshSettings = state.meshManager.managedMeshSettings()
            val numScaleLevels = state.source.numMipmapLevels
            val meshInfos = MeshInfos(
                    selectedSegments,
                    assignment,
                    meshManager,
                    meshSettings,
                    numScaleLevels
            )
            LOG.debug(
                    "Creating mesh pane for source {} from {} and {}: ",
                    state.nameProperty().get(),
                    meshManager,
                    meshInfos
            )
            return MeshPane(meshManager, meshInfos, numScaleLevels)
        }
    }
}