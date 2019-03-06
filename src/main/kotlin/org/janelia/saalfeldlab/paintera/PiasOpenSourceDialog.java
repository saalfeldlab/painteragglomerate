package org.janelia.saalfeldlab.paintera;

import javafx.beans.binding.Bindings;
import javafx.beans.binding.BooleanBinding;
import javafx.beans.property.DoubleProperty;
import javafx.beans.value.ObservableObjectValue;
import javafx.beans.value.ObservableValue;
import javafx.geometry.Insets;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.Dialog;
import javafx.scene.control.Label;
import javafx.scene.control.TitledPane;
import javafx.scene.control.Tooltip;
import javafx.scene.effect.InnerShadow;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;
import org.janelia.saalfeldlab.fx.util.InvokeOnJavaFXApplicationThread;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.paintera.ui.opendialog.CombinesErrorMessages;
import org.janelia.saalfeldlab.paintera.ui.opendialog.NameField;
import org.janelia.saalfeldlab.paintera.ui.opendialog.meta.MetaPanel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Consumer;

public class PiasOpenSourceDialog extends Dialog<GenericPiasDialogN5> implements CombinesErrorMessages {

	private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

	private final Label errorMessage;

	private final NameField nameField = new NameField(
			"Source name",
			"Specify source name (required)",
			new InnerShadow(10, Color.ORANGE)
	);

	private final BooleanBinding isError;

	private final GenericPiasDialogN5 backendDialog;

	private final MetaPanel metaPanel = new MetaPanel();

	public PiasOpenSourceDialog(final GenericPiasDialogN5 backendDialog) {
		super();

		this.backendDialog = backendDialog;
		this.metaPanel.listenOnDimensions(backendDialog.dimensionsProperty());
		this.backendDialog.axisOrder.bind(this.metaPanel.axisOrderProperty());

		this.setTitle("Open data set");
		this.setHeaderText("Open PIAS data set");
		this.getDialogPane().getButtonTypes().addAll(ButtonType.CANCEL, ButtonType.OK);
		((Button) this.getDialogPane().lookupButton(ButtonType.CANCEL)).setText("_Cancel");
		((Button) this.getDialogPane().lookupButton(ButtonType.OK)).setText("_OK");
		this.errorMessage = new Label("");
		TitledPane errorInfo = new TitledPane("", errorMessage);
		this.isError = Bindings.createBooleanBinding(() -> Optional.ofNullable(this.errorMessage.textProperty().get()).orElse("").length() > 0, this.errorMessage.textProperty());
		errorInfo.textProperty().bind(Bindings.createStringBinding(() -> this.isError.get() ? "ERROR" : "", this.isError));

		this.getDialogPane().lookupButton(ButtonType.OK).disableProperty().bind(this.isError);
		errorInfo.visibleProperty().bind(this.isError);

		GridPane grid = new GridPane();
		this.nameField.errorMessageProperty().addListener((obs, oldv, newv) -> combineErrorMessages());
		VBox dialogContent = new VBox(10, nameField.textField(), grid, metaPanel.getPane(), errorInfo);
		this.setResizable(true);

		GridPane.setMargin(this.backendDialog.getDialogNode(), new Insets(0, 0, 0, 30));
		grid.add(this.backendDialog.getDialogNode(), 1, 0);
		GridPane.setHgrow(this.backendDialog.getDialogNode(), Priority.ALWAYS);

		this.getDialogPane().setContent(dialogContent);

		final ObservableObjectValue<DatasetAttributes> attributesProperty = backendDialog.datsetAttributesProperty();

		final DoubleProperty[] res = backendDialog.resolution();
		final DoubleProperty[] off = backendDialog.offset();
		this.metaPanel.listenOnResolution(res[0], res[1], res[2]);
		this.metaPanel.listenOnOffset(off[0], off[1], off[2]);
		this.metaPanel.listenOnMinMax(backendDialog.min(), backendDialog.max());
		backendDialog.errorMessage().addListener((obs, oldErr, newErr) -> combineErrorMessages());
		backendDialog.nameProperty().addListener((obs, oldName, newName) -> Optional.ofNullable(newName).ifPresent(nameField.textField().textProperty()::set));
		combineErrorMessages();
		Optional.ofNullable(backendDialog.nameProperty().get()).ifPresent(nameField.textField()::setText);

		metaPanel.listenOnResolution(backendDialog.resolution()[0], backendDialog.resolution()[1], backendDialog.resolution()[2]);

		metaPanel.getRevertButton().setOnAction(event -> {
			backendDialog.setResolution(revert(metaPanel.getResolution()));
			backendDialog.setOffset(revert(metaPanel.getOffset()));
		});

		this.setResultConverter(button -> button.equals(ButtonType.OK) ? backendDialog : null);
		combineErrorMessages();
		setTitle(Paintera.NAME);

	}

	public MetaPanel.TYPE getType() {
		return MetaPanel.TYPE.LABEL;
	}

	public String getName() {
		return nameField.getText();
	}

	public MetaPanel getMeta() {
		return this.metaPanel;
	}

	@Override
	public Collection<ObservableValue<String>> errorMessages() {
		return Arrays.asList(this.nameField.errorMessageProperty(), backendDialog.errorMessage());
	}

	@Override
	public Consumer<Collection<String>> combiner() {
		return strings -> InvokeOnJavaFXApplicationThread.invoke(() -> this.errorMessage.setText(String.join(
				"\n",
				strings
		)));
	}

	private static double[] revert(final double[] array) {
		final double[] reverted = new double[array.length];
		for (int i = 0; i < array.length; ++i) {
			reverted[i] = array[array.length - 1 - i];
		}
		return reverted;
	}
}
