/**
 * 
 */
package mil.nga.giat.geowave.datastore.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.IngestCallback;
import mil.nga.giat.geowave.core.store.IngestCallbackList;
import mil.nga.giat.geowave.core.store.ScanCallback;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.IndexDependentDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatsCompositionTool;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.data.visibility.UnconstrainedVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.visibility.UniformVisibilityWriter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.hbase.io.HBaseWriter;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseIteratorWrapper;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseIteratorWrapper.Callback;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseIteratorWrapper.Converter;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils;

import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.log4j.Logger;

/**
 * @author viggy
 * 
 */
public class HBaseDataStore implements
		DataStore
{

	private final static Logger LOGGER = Logger.getLogger(HBaseDataStore.class);
	private HBaseIndexStore indexStore;
	private HBaseAdapterStore adapterStore;
	private BasicHBaseOperations operations;
	private HBaseDataStatisticsStore statisticsStore;

	public HBaseDataStore(
			HBaseIndexStore indexStore,
			HBaseAdapterStore adapterStore,
			HBaseDataStatisticsStore statisticsStore,
			BasicHBaseOperations operations ) {
		this.indexStore = indexStore;
		this.adapterStore = adapterStore;
		this.statisticsStore = statisticsStore;
		this.operations = operations;
	}

	public HBaseDataStore(BasicHBaseOperations operations) {
		this(
				new HBaseIndexStore(
						operations),
				new HBaseAdapterStore(
						operations),
				new HBaseDataStatisticsStore(
						operations),
				operations);
	}

	@Override
	public <T> IndexWriter createIndexWriter(
			Index index ) {
		return new HBaseIndexWriter(
				index,
				operations,
				this);
	}

	@Override
	public <T> List<ByteArrayId> ingest(
			WritableDataAdapter<T> writableAdapter,
			Index index,
			T entry ) {
		return this.ingest(
				writableAdapter,
				index,
				entry,
				new UniformVisibilityWriter<T>(
						new UnconstrainedVisibilityHandler<T, Object>()));
	}

	@Override
	public <T> void ingest(
			WritableDataAdapter<T> writableAdapter,
			Index index,
			Iterator<T> entryIterator ) {
		ingest(
				writableAdapter,
				index,
				entryIterator,
				null,
				new UniformVisibilityWriter<T>(
						new UnconstrainedVisibilityHandler<T, Object>()));
	}

	public <T> void ingest(
			final WritableDataAdapter<T> writableAdapter,
			final Index index,
			Iterator<T> entryIterator,
			IngestCallback<T> ingestCallback,
			VisibilityWriter<T> customFieldVisibilityWriter ) {
		if (writableAdapter instanceof IndexDependentDataAdapter) {
			ingestInternal(
					writableAdapter,
					index,
					new HBaseIteratorWrapper<T, T>(
							entryIterator,
							new Converter<T, T>() {

								@Override
								public Iterator<T> convert(
										final T entry ) {
									return ((IndexDependentDataAdapter) writableAdapter).convertToIndex(
											index,
											entry);
								}
							},
							null),
					ingestCallback,
					customFieldVisibilityWriter);
		}
		else {
			ingestInternal(
					writableAdapter,
					index,
					entryIterator,
					ingestCallback,
					customFieldVisibilityWriter);
		}
	}

	@Override
	public CloseableIterator<?> query(
			Query query ) {
		// TODO #406 Need to fix
		LOGGER.error("This method is not yet coded. Need to fix it");
		return null;
	}

	@Override
	public <T> T getEntry(
			Index index,
			ByteArrayId rowId ) {
		// TODO #406 Need to fix
		LOGGER.error("This method is not yet coded. Need to fix it");
		return null;
	}

	@Override
	public <T> T getEntry(
			Index index,
			ByteArrayId dataId,
			ByteArrayId adapterId,
			String... additionalAuthorizations ) {
		// TODO #406 Need to fix
		LOGGER.error("This method is not yet coded. Need to fix it");
		return null;
	}

	@Override
	public boolean deleteEntry(
			Index index,
			ByteArrayId dataId,
			ByteArrayId adapterId,
			String... authorizations ) {
		// TODO #406 Need to fix
		LOGGER.error("This method is not yet coded. Need to fix it");
		return false;
	}

	@Override
	public <T> CloseableIterator<T> getEntriesByPrefix(
			Index index,
			ByteArrayId rowPrefix,
			String... authorizations ) {
		// TODO #406 Need to fix
		LOGGER.error("This method is not yet coded. Need to fix it");
		return null;
	}

	@Override
	public <T> CloseableIterator<T> query(
			DataAdapter<T> adapter,
			Query query ) {
		// TODO #406 Need to fix
		LOGGER.error("This method is not yet coded. Need to fix it");
		return null;
	}

	@Override
	public <T> CloseableIterator<T> query(
			Index index,
			Query query ) {
		// TODO #406 Need to fix
		LOGGER.error("This method is not yet coded. Need to fix it");
		return null;
	}

	@Override
	public <T> CloseableIterator<T> query(
			Index index,
			Query query,
			QueryOptions queryOptions ) {
		// TODO #406 Need to fix
		LOGGER.error("This method is not yet coded. Need to fix it");
		return null;
	}

	@Override
	public <T> CloseableIterator<T> query(
			DataAdapter<T> adapter,
			Index index,
			Query query ) {
		// TODO #406 Need to fix
		LOGGER.error("This method is not yet coded. Need to fix it");
		return null;
	}

	@Override
	public CloseableIterator<?> query(
			List<ByteArrayId> adapterIds,
			Query query ) {
		// TODO #406 Need to fix
		LOGGER.error("This method is not yet coded. Need to fix it");
		return null;
	}

	@Override
	public CloseableIterator<?> query(
			Query query,
			int limit ) {
		// TODO #406 Need to fix
		LOGGER.error("This method is not yet coded. Need to fix it");
		return null;
	}

	@Override
	public <T> CloseableIterator<T> query(
			DataAdapter<T> adapter,
			Query query,
			int limit ) {
		// TODO #406 Need to fix
		LOGGER.error("This method is not yet coded. Need to fix it");
		return null;
	}

	@Override
	public <T> CloseableIterator<T> query(
			Index index,
			Query query,
			int limit ) {
		// TODO #406 Need to fix
		LOGGER.error("This method is not yet coded. Need to fix it");
		return null;
	}

	@Override
	public <T> CloseableIterator<T> query(
			DataAdapter<T> adapter,
			Index index,
			Query query,
			int limit ) {
		// TODO #406 Need to fix
		LOGGER.error("This method is not yet coded. Need to fix it");
		return null;
	}

	@Override
	public CloseableIterator<?> query(
			List<ByteArrayId> adapterIds,
			Query query,
			int limit ) {
		// TODO #406 Need to fix
		LOGGER.error("This method is not yet coded. Need to fix it");
		return null;
	}

	@Override
	public <T> CloseableIterator<T> query(
			DataAdapter<T> adapter,
			Index index,
			Query query,
			int limit,
			String... authorizations ) {
		// TODO #406 Need to fix
		LOGGER.error("This method is not yet coded. Need to fix it");
		return null;
	}

	@Override
	public <T> CloseableIterator<T> query(
			DataAdapter<T> adapter,
			Index index,
			Query query,
			Integer limit,
			ScanCallback<?> scanCallback,
			String... authorizations ) {
		// TODO #406 Need to fix
		LOGGER.error("This method is not yet coded. Need to fix it");
		return null;
	}

	private <T> void ingestInternal(
			final WritableDataAdapter<T> dataWriter,
			final Index index,
			final Iterator<T> entryIterator,
			final IngestCallback<T> ingestCallback,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {

		try {

			final String indexName = StringUtils.stringFromBinary(index.getId().getBytes());
			HBaseWriter writer = operations.createWriter(indexName);

			final List<IngestCallback<T>> callbacks = new ArrayList<IngestCallback<T>>();
			final StatsCompositionTool<T> statsCompositionTool = this.getStatsCompositionTool(dataWriter);
			callbacks.add(statsCompositionTool);

			if (ingestCallback != null) {
				callbacks.add(ingestCallback);
			}
			final IngestCallback<T> finalIngestCallback;
			if (callbacks.size() > 1) {
				finalIngestCallback = new IngestCallbackList<T>(
						callbacks);
			}
			else if (callbacks.size() == 1) {
				finalIngestCallback = callbacks.get(0);
			}
			else {
				finalIngestCallback = null;
			}

			writer.write(new Iterable<RowMutations>() {
				@Override
				public Iterator<RowMutations> iterator() {
					return new HBaseIteratorWrapper<T, RowMutations>(
							entryIterator,
							new Converter<T, RowMutations>() {

								@Override
								public Iterator<RowMutations> convert(
										final T entry ) {
									return HBaseUtils.entryToMutations(
											dataWriter,
											index,
											entry,
											customFieldVisibilityWriter).iterator();
								}
							},
							finalIngestCallback == null ? null : new Callback<T, RowMutations>() {

								@Override
								public void notifyIterationComplete(
										final T entry ) {
									finalIngestCallback.entryIngested(
											HBaseUtils.getIngestInfo(
													dataWriter,
													index,
													entry,
													customFieldVisibilityWriter),
											entry);
								}
							});
				}
			});
			writer.close();

			synchronizeStatsWithStore(
					statsCompositionTool,
					true);
		}
		catch (IOException e) {
			LOGGER.warn(
					"Unable to create writer",
					e);
		}

	}

	private <T> StatsCompositionTool<T> getStatsCompositionTool(
			final DataAdapter<T> adapter ) {
		return new StatsCompositionTool<T>(
				adapter,
				null);
	}

	private <T> void synchronizeStatsWithStore(
			StatsCompositionTool<T> compositionTool,
			boolean commitStats ) {
		if (commitStats)
			compositionTool.flush();
		else
			compositionTool.reset();
	}

	@Override
	public <T> List<ByteArrayId> ingest(
			WritableDataAdapter<T> writableAdapter,
			Index index,
			T entry,
			VisibilityWriter<T> customFieldVisibilityWriter ) {
		if (writableAdapter instanceof IndexDependentDataAdapter) {
			final IndexDependentDataAdapter adapter = ((IndexDependentDataAdapter) writableAdapter);
			final Iterator<T> indexedEntries = adapter.convertToIndex(
					index,
					entry);
			final List<ByteArrayId> rowIds = new ArrayList<ByteArrayId>();
			while (indexedEntries.hasNext()) {
				rowIds.addAll(ingestInternal(
						adapter,
						index,
						indexedEntries.next(),
						customFieldVisibilityWriter));
			}
			return rowIds;
		}
		else {
			return ingestInternal(
					writableAdapter,
					index,
					entry,
					customFieldVisibilityWriter);
		}
	}

	public <T> List<ByteArrayId> ingestInternal(
			final WritableDataAdapter<T> writableAdapter,
			final Index index,
			final T entry,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {

		HBaseWriter writer = null;
		StatsCompositionTool<T> statisticsTool = null;
		final String indexName = StringUtils.stringFromBinary(index.getId().getBytes());
		final String altIdxTableName = indexName + HBaseUtils.ALT_INDEX_TABLE;
		final byte[] adapterId = writableAdapter.getAdapterId().getBytes();

		statisticsTool = getStatsCompositionTool(writableAdapter);

		try {
			writer = operations.createWriter(indexName);
		}
		catch (IOException e) {
			LOGGER.warn(
					"Unable to create writer'" + indexName + "'",
					e);
		}

		final DataStoreEntryInfo entryInfo = HBaseUtils.write(
				writableAdapter,
				index,
				entry,
				writer,
				customFieldVisibilityWriter);

		writer.close();

		statisticsTool.entryIngested(
				entryInfo,
				entry);

		synchronizeStatsWithStore(
				statisticsTool,
				true);

		return entryInfo.getRowIds();
	}

	@Override
	public <T> void ingest(
			WritableDataAdapter<T> writableAdapter,
			Index index,
			Iterator<T> entryIterator,
			IngestCallback<T> ingestCallback ) {
		// TODO #406 Need to fix
	}

	public void store(Index index) {
		if (!indexStore.indexExists(index.getId())) {
			indexStore.addIndex(index);
		}
	}
	
	protected synchronized void store(
			final DataAdapter<?> adapter ) {
		if (!adapterStore.adapterExists(adapter.getAdapterId())) {
			adapterStore.addAdapter(adapter);
		}
	}
}
