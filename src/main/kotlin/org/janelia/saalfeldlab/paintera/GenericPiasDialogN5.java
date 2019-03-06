package org.janelia.saalfeldlab.paintera;

import javafx.beans.binding.Bindings;
import javafx.beans.binding.BooleanBinding;
import javafx.beans.binding.ObjectBinding;
import javafx.beans.binding.StringBinding;
import javafx.beans.property.BooleanProperty;
import javafx.beans.property.DoubleProperty;
import javafx.beans.property.IntegerProperty;
import javafx.beans.property.LongProperty;
import javafx.beans.property.ObjectProperty;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.beans.property.SimpleIntegerProperty;
import javafx.beans.property.SimpleLongProperty;
import javafx.beans.property.SimpleObjectProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.property.StringProperty;
import javafx.beans.value.ObservableObjectValue;
import javafx.beans.value.ObservableStringValue;
import javafx.beans.value.ObservableValue;
import javafx.scene.Group;
import javafx.scene.Node;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.Label;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import net.imglib2.Cursor;
import net.imglib2.Interval;
import net.imglib2.Volatile;
import net.imglib2.algorithm.util.Grids;
import net.imglib2.cache.img.CachedCellImg;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.type.NativeType;
import net.imglib2.type.label.LabelUtils;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;
import org.controlsfx.control.StatusBar;
import org.janelia.saalfeldlab.fx.ui.ExceptionNode;
import org.janelia.saalfeldlab.fx.ui.NumberField;
import org.janelia.saalfeldlab.fx.ui.ObjectField;
import org.janelia.saalfeldlab.fx.util.InvokeOnJavaFXApplicationThread;
import org.janelia.saalfeldlab.labels.blocks.LabelBlockLookup;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.paintera.cache.global.GlobalCache;
import org.janelia.saalfeldlab.paintera.composition.ARGBCompositeAlphaYCbCr;
import org.janelia.saalfeldlab.paintera.control.assignment.FragmentSegmentAssignmentState;
import org.janelia.saalfeldlab.paintera.control.lock.LockedSegmentsOnlyLocal;
import org.janelia.saalfeldlab.paintera.control.selection.SelectedIds;
import org.janelia.saalfeldlab.paintera.data.DataSource;
import org.janelia.saalfeldlab.paintera.data.axisorder.AxisOrder;
import org.janelia.saalfeldlab.paintera.data.mask.Masks;
import org.janelia.saalfeldlab.paintera.data.mask.persist.PersistCanvas;
import org.janelia.saalfeldlab.paintera.data.n5.CommitCanvasN5;
import org.janelia.saalfeldlab.paintera.data.n5.ReflectionException;
import org.janelia.saalfeldlab.paintera.id.IdService;
import org.janelia.saalfeldlab.paintera.id.N5IdService;
import org.janelia.saalfeldlab.paintera.meshes.InterruptibleFunction;
import org.janelia.saalfeldlab.paintera.meshes.MeshManagerWithAssignmentForSegments;
import org.janelia.saalfeldlab.paintera.state.LabelSourceState;
import org.janelia.saalfeldlab.paintera.stream.HighlightingStreamConverter;
import org.janelia.saalfeldlab.paintera.stream.ModalGoldenAngleSaturatedHighlightingARGBStream;
import org.janelia.saalfeldlab.paintera.ui.PainteraAlerts;
import org.janelia.saalfeldlab.paintera.ui.opendialog.DatasetInfo;
import org.janelia.saalfeldlab.paintera.ui.opendialog.meta.ChannelInformation;
import org.janelia.saalfeldlab.util.MakeUnchecked;
import org.janelia.saalfeldlab.util.NamedThreadFactory;
import org.janelia.saalfeldlab.util.n5.N5Data;
import org.janelia.saalfeldlab.util.n5.N5Helpers;
import org.janelia.saalfeldlab.util.n5.N5Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.IntFunction;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public class GenericPiasDialogN5
{

	private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

	private static final String MIN_KEY = "min";

	private static final String MAX_KEY = "max";

	private static final String ERROR_MESSAGE_PATTERN = "ping server? -- %s -- n5? %s -- dataset? %s";

	private final DatasetInfo datasetInfo = new DatasetInfo();

	private final SimpleObjectProperty<Supplier<N5Writer>> n5Supplier = new SimpleObjectProperty<>(() -> null);

	private final ObjectBinding<N5Writer> n5 = Bindings.createObjectBinding(() -> Optional
			.ofNullable(n5Supplier.get())
			.map(Supplier::get)
			.orElse(null), n5Supplier);

	private final StringProperty dataset = new SimpleStringProperty();

	private final BooleanBinding isN5Valid = n5.isNotNull();

	private final BooleanBinding isDatasetValid = dataset.isNotNull();

	private final ObjectProperty<DatasetAttributes> datasetAttributes = new SimpleObjectProperty<>();

	private final ObjectBinding<long[]> dimensions = Bindings.createObjectBinding(() -> Optional
			.ofNullable(datasetAttributes.get())
			.map(DatasetAttributes::getDimensions)
			.orElse(null), datasetAttributes);

	final ObjectProperty<AxisOrder> axisOrder = new SimpleObjectProperty<>();

	private final ChannelInformation channelInformation = new ChannelInformation();

	private final BooleanBinding isReady = isN5Valid.and(isDatasetValid);

	private final StringBinding errorMessage = Bindings.createStringBinding(
			() -> isReady.get()
			      ? null
			      : String.format(ERROR_MESSAGE_PATTERN, isN5Valid.get(), isDatasetValid.get()),
			isReady);

	private final StringBinding name = Bindings.createStringBinding(() -> {
		final String[] entries = Optional
				.ofNullable(dataset.get())
				.map(d -> d.split("/")).filter(a -> a.length > 0).orElse(new String[]{null});
		return entries[entries.length - 1];
	}, dataset);

	private final Node node;

	public GenericPiasDialogN5(
			final Node n5RootNode,
			final Node browseNode,
			final ObservableValue<Supplier<N5Writer>> writerSupplier)
	{
		this.node = initializeNode(n5RootNode, browseNode);
		n5Supplier.bind(writerSupplier);
		dataset.addListener((obs, oldv, newv) -> Optional.ofNullable(newv).filter(v -> v.length() > 0).ifPresent(v ->
				updateDatasetInfo(
				v,
				this.datasetInfo)));

		dataset.set("");
	}

	ObservableObjectValue<DatasetAttributes> datsetAttributesProperty()
	{
		return this.datasetAttributes;
	}

	ObservableObjectValue<long[]> dimensionsProperty()
	{
		return this.dimensions;
	}

	private void updateDatasetInfo(final String group, final DatasetInfo info)
	{

		LOG.debug("Updating dataset info for dataset {}", group);
		try
		{
			final N5Reader n5 = this.n5.get();

			setResolution(N5Helpers.getResolution(n5, group));
			setOffset(N5Helpers.getOffset(n5, group));

			this.datasetAttributes.set(N5Helpers.getDatasetAttributes(n5, group));

			final DataType dataType = N5Types.getDataType(n5, group);

			this.datasetInfo.minProperty().set(Optional.ofNullable(n5.getAttribute(
					group,
					MIN_KEY,
					Double.class)).orElse(N5Types.minForType(dataType)));
			this.datasetInfo.maxProperty().set(Optional.ofNullable(n5.getAttribute(
					group,
					MAX_KEY,
					Double.class)).orElse(N5Types.maxForType(dataType)));
		} catch (final IOException e)
		{
			ExceptionNode.exceptionDialog(e).show();
		}
	}

	Node getDialogNode()
	{
		return node;
	}

	StringBinding errorMessage()
	{
		return errorMessage;
	}

	DoubleProperty[] resolution()
	{
		return this.datasetInfo.spatialResolutionProperties();
	}

	DoubleProperty[] offset()
	{
		return this.datasetInfo.spatialOffsetProperties();
	}

	DoubleProperty min()
	{
		return this.datasetInfo.minProperty();
	}

	DoubleProperty max()
	{
		return this.datasetInfo.maxProperty();
	}

	private FragmentSegmentAssignmentState assignments() throws IOException
	{
		return N5Helpers.assignments(n5.get(), this.dataset.get());
	}

	private IdService idService() throws IOException
	{
		try {
			LOG.warn("Getting id service for {} -- {}", this.n5.get(), this.dataset.get());
			return N5Helpers.idService(this.n5.get(), this.dataset.get());
		} catch (final N5Helpers.MaxIDNotSpecified e) {
			final Alert alert = PainteraAlerts.alert(Alert.AlertType.CONFIRMATION);
			alert.setHeaderText("maxId not specified in dataset.");
			final TextArea ta = new TextArea("Could not read maxId attribute from data set. " +
					"You can specify the max id manually, or read it from the data set (this can take a long time if your data is big).\n" +
					"Alternatively, press cancel to load the data set without an id service. " +
					"Fragment-segment-assignments and selecting new (wrt to the data) labels require an id service " +
					"and will not be available if you press cancel.");
			ta.setEditable(false);
			ta.setWrapText(true);
			final NumberField<LongProperty> nextIdField = NumberField.longField(0, v -> true, ObjectField.SubmitOn.ENTER_PRESSED, ObjectField.SubmitOn.FOCUS_LOST);
			final Button scanButton = new Button("Scan Data");
			scanButton.setOnAction(event -> {
				event.consume();
				try {
					findMaxId(this.n5.get(), this.dataset.getValue(), nextIdField.valueProperty()::set);
				} catch (IOException e1) {
					throw new RuntimeException(e1);
				}
			});
			final HBox maxIdBox = new HBox(new Label("Max Id:"), nextIdField.textField(), scanButton);
			HBox.setHgrow(nextIdField.textField(), Priority.ALWAYS);
			alert.getDialogPane().setContent(new VBox(ta, maxIdBox));
			final Optional<ButtonType> bt = alert.showAndWait();
			if (bt.isPresent() && ButtonType.OK.equals(bt.get())) {
				long maxId = nextIdField.valueProperty().get() + 1;
				this.n5.get().setAttribute(dataset.get(), "maxId", maxId);
				return new N5IdService(this.n5.get(), this.dataset.get(), maxId);
			}
			else
				return new IdService.IdServiceNotProvided();
		}
	}

	private static <I extends IntegerType<I> & NativeType<I>> void findMaxId(
			final N5Reader reader,
			String dataset,
			final LongConsumer maxIdTarget
			) throws IOException {
		final int numProcessors = Runtime.getRuntime().availableProcessors();
		final ExecutorService es = Executors.newFixedThreadPool(numProcessors, new NamedThreadFactory("max-id-discovery-%d", true));
		dataset = N5Helpers.isPainteraDataset(reader, dataset) ? dataset + "/data" : dataset;
		dataset = N5Helpers.isMultiScale(reader, dataset) ? N5Helpers.getFinestLevelJoinWithGroup(reader, dataset) : dataset;
		final boolean isLabelMultiset = N5Helpers.getBooleanAttribute(reader, dataset, N5Helpers.IS_LABEL_MULTISET_KEY, false);
		final CachedCellImg<I, ?> img = isLabelMultiset ? (CachedCellImg<I, ?>) (CachedCellImg) LabelUtils.openVolatile(reader, dataset) : (CachedCellImg<I, ?>)N5Utils.open(reader, dataset);
		final int[] blockSize = new  int[img.numDimensions()];
		img.getCellGrid().cellDimensions(blockSize);
		final List<Interval> blocks = Grids.collectAllContainedIntervals(img.getCellGrid().getImgDimensions(), blockSize);
		final IntegerProperty completedTasks = new SimpleIntegerProperty(0);
		final LongProperty maxId = new SimpleLongProperty(0);
		final BooleanProperty wasCanceled = new SimpleBooleanProperty(false);
		LOG.debug("Scanning for max id over {} blocks of size {} (total size {}).", blocks.size(), blockSize, img.getCellGrid().getImgDimensions());
		final Thread t = new Thread(() -> {
			List<Callable<Long>> tasks = new ArrayList<>();
			for (final Interval block : blocks) {
				tasks.add(() -> {
					long localMaxId = 0;
					try {
						final IntervalView<I> interval = Views.interval(img, block);
						final Cursor<I> cursor = interval.cursor();
						while (cursor.hasNext() && !wasCanceled.get()) {
							final long id = cursor.next().getIntegerLong();
							if (id > localMaxId)
								localMaxId = id;
						}
						return localMaxId;
					}
					finally {
						synchronized (completedTasks) {
							if (!wasCanceled.get()) {
								LOG.trace("Incrementing completed tasks ({}/{}) and maxId ({}) with {}", completedTasks, blocks.size(), maxId, localMaxId);
								maxId.setValue(Math.max(maxId.getValue(), localMaxId));
								completedTasks.set(completedTasks.get() + 1);
							}
						}
					}
				});
			}
			try {
				final List<Future<Long>> futures = es.invokeAll(tasks);
				for (final Future<Long> future : futures) {
					final Long id = future.get();
				}
			} catch (final InterruptedException | ExecutionException e) {
				synchronized (completedTasks) {
					completedTasks.set(-1);
					wasCanceled.set(true);
				}
				LOG.error("Was interrupted while finding max id.", e);
			}
		});
		t.setName("max-id-discovery-main-thread");
		t.setDaemon(true);
		t.start();
		final Alert alert = PainteraAlerts.alert(Alert.AlertType.CONFIRMATION, true);
		alert.setHeaderText("Finding max id...");
		final BooleanBinding stillRuning = Bindings.createBooleanBinding(() -> completedTasks.get() < blocks.size(), completedTasks);
		alert.getDialogPane().lookupButton(ButtonType.OK).disableProperty().bind(stillRuning);
		final StatusBar statusBar = new StatusBar();
		completedTasks.addListener((obs, oldv, newv) -> InvokeOnJavaFXApplicationThread.invoke(() -> statusBar.setProgress(newv.doubleValue() / blocks.size())));
		completedTasks.addListener((obs, oldv, newv) -> InvokeOnJavaFXApplicationThread.invoke(() -> statusBar.setText(String.format("%s/%d", newv, blocks.size()))));
		alert.getDialogPane().setContent(statusBar);
		wasCanceled.addListener((obs, oldv, newv) -> InvokeOnJavaFXApplicationThread.invoke(() -> alert.setHeaderText("Cancelled")));
		completedTasks.addListener((obs, oldv, newv) -> InvokeOnJavaFXApplicationThread.invoke(() -> alert.setHeaderText(newv.intValue() < blocks.size() ? "Finding max id: " + maxId.getValue() : "Found max id: " + maxId.getValue())));
		final Optional<ButtonType> bt = alert.showAndWait();
		if (bt.isPresent() && ButtonType.OK.equals(bt.get())) {
			LOG.warn("Setting max id to {}", maxId.get());
			maxIdTarget.accept(maxId.get());
		} else {
			wasCanceled.set(true);
		}

	}

	public ChannelInformation getChannelInformation()
	{
		return this.channelInformation;
	}

	private Node initializeNode(
			final Node rootNode,
			final Node browseNode)
	{

		final TextField dataset = new TextField();
		dataset.setEditable(false);
		dataset.textProperty().bind(this.dataset);
		final GridPane grid = new GridPane();
		grid.add(rootNode, 0, 0);
		grid.add(dataset, 0, 1);
		GridPane.setHgrow(rootNode, Priority.ALWAYS);
		GridPane.setHgrow(dataset, Priority.ALWAYS);
		grid.add(browseNode, 1, 0);
		return grid;
	}

	ObservableStringValue nameProperty()
	{
		return name;
	}

	<D extends NativeType<D> & IntegerType<D>, T extends Volatile<D> & NativeType<T>> LabelSourceState<D, T>
	getLabels(
			final String name,
			final GlobalCache globalCache,
			final int priority,
			final Group meshesGroup,
			final ExecutorService manager,
			final ExecutorService workers,
			final String projectDirectory) throws IOException, ReflectionException {
		final N5Writer          reader     = n5.get();
		final String            dataset    = this.dataset.get();
		final double[]          resolution = asPrimitiveArray(resolution());
		final double[]          offset     = asPrimitiveArray(offset());
		final AffineTransform3D transform  = N5Helpers.fromResolutionAndOffset(resolution, offset);
		final DataSource<D, T>  source;
		if (N5Types.isLabelMultisetType(reader, dataset))
		{
			source = (DataSource) N5Data.openLabelMultisetAsSource(
					reader,
					dataset,
					transform,
					globalCache,
					priority,
					name);
		}
		else
		{
			LOG.debug("Getting scalar data source");
			source = N5Data.openScalarAsSource(
					reader,
					dataset,
					transform,
					globalCache,
					priority,
					name);
		}

		final Supplier<String> canvasCacheDirUpdate = Masks.canvasTmpDirDirectorySupplier(projectDirectory);

		final DataSource<D, T>               masked         = Masks.mask(
				source,
				canvasCacheDirUpdate.get(),
				canvasCacheDirUpdate,
				commitCanvas(),
				workers
		                                                                );
		final IdService                      idService      = idService();
		final FragmentSegmentAssignmentState assignment     = assignments();
		final SelectedIds                    selectedIds    = new SelectedIds();
		final LockedSegmentsOnlyLocal        lockedSegments = new LockedSegmentsOnlyLocal(locked -> {
		});
		final ModalGoldenAngleSaturatedHighlightingARGBStream stream = new
				ModalGoldenAngleSaturatedHighlightingARGBStream(
				selectedIds,
				assignment,
				lockedSegments
		);
		final HighlightingStreamConverter<T> converter = HighlightingStreamConverter.forType(stream, masked.getType());

		final LabelBlockLookup lookup =  getLabelBlockLookup(n5.get(), dataset, source);

		IntFunction<InterruptibleFunction<Long, Interval[]>> loaderForLevelFactory = level -> InterruptibleFunction.fromFunction(
				MakeUnchecked.function(
						id -> lookup.read(level, id),
						id -> {LOG.debug("Falling back to empty array"); return new Interval[0];}
		));

		InterruptibleFunction<Long, Interval[]>[] blockLoaders = IntStream
				.range(0, masked.getNumMipmapLevels())
				.mapToObj(loaderForLevelFactory)
				.toArray(InterruptibleFunction[]::new );

		MeshManagerWithAssignmentForSegments meshManager = MeshManagerWithAssignmentForSegments.fromBlockLookup(
				masked,
				selectedIds,
				assignment,
				stream,
				meshesGroup,
				blockLoaders,
				globalCache::createNewCache,
				manager,
				workers);

		return new LabelSourceState<>(
				masked,
				converter,
				new ARGBCompositeAlphaYCbCr(),
				name,
				assignment,
				lockedSegments,
				idService,
				selectedIds,
				meshManager,
				lookup);
	}

	private PersistCanvas commitCanvas() throws IOException {
		final String   dataset = this.dataset.get();
		final N5Writer writer  = this.n5.get();
		return new CommitCanvasN5(writer, dataset);
	}

	private double[] asPrimitiveArray(final DoubleProperty[] data)
	{
		return Arrays.stream(data).mapToDouble(DoubleProperty::get).toArray();
	}

	void setResolution(final double[] resolution)
	{
		final DoubleProperty[] res = resolution();
		for (int i = 0; i < res.length; ++i)
		{
			res[i].set(resolution[i]);
		}
	}

	void setOffset(final double[] offset)
	{
		final DoubleProperty[] off = offset();
		for (int i = 0; i < off.length; ++i)
		{
			off[i].set(offset[i]);
		}
	}

	private static LabelBlockLookup getLabelBlockLookup(final N5Reader reader, final String dataset, final DataSource<?, ?> fallBack) throws IOException {
		try {
			return N5Helpers.getLabelBlockLookup(reader, dataset);
		} catch (N5Helpers.NotAPainteraDataset e) {
			return PainteraAlerts.getLabelBlockLookupFromDataSource(fallBack);
		}
	}
}
