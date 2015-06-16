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
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.IndexDependentDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.hbase.io.HBaseWriter;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils;

import org.apache.log4j.Logger;

/**
 * @author viggy
 * 
 */
public class HBaseIndexWriter implements
		IndexWriter
{

	private final static Logger LOGGER = Logger.getLogger(HBaseIndexWriter.class);
	private Index index;
	private HBaseDataStore dataStore;
	private BasicHBaseOperations operations;
	private String indexName;
	protected HBaseWriter writer;

	public HBaseIndexWriter(
			final Index index,
			final BasicHBaseOperations operations,
			final HBaseDataStore dataStore ) {
		this.index = index;
		this.operations = operations;
		this.dataStore = dataStore;
		initialize();
	}

	private void initialize() {
		indexName = StringUtils.stringFromBinary(index.getId().getBytes());
	}

	@Override
	public void close()
			throws IOException {
		// thread safe close
		closeInternal();
	}

	private synchronized void closeInternal() {
		if (writer != null) {
			writer.close();
			writer = null;
		}
	}

	@Override
	public <T> List<ByteArrayId> write(
			WritableDataAdapter<T> writableAdapter,
			T entry ) {
		if (writableAdapter instanceof IndexDependentDataAdapter) {
			final IndexDependentDataAdapter adapter = ((IndexDependentDataAdapter) writableAdapter);
			final Iterator<T> indexedEntries = adapter.convertToIndex(
					index,
					entry);
			final List<ByteArrayId> rowIds = new ArrayList<ByteArrayId>();
			while (indexedEntries.hasNext()) {
				rowIds.addAll(writeInternal(
						adapter,
						indexedEntries.next()));
			}
			return rowIds;
		}
		else {
			return writeInternal(
					writableAdapter,
					entry);
		}
	}

	public <T> List<ByteArrayId> writeInternal(
			final WritableDataAdapter<T> writableAdapter,
			final T entry ) {
		final ByteArrayId adapterIdObj = writableAdapter.getAdapterId();

		final byte[] adapterId = writableAdapter.getAdapterId().getBytes();

		DataStoreEntryInfo entryInfo;
		synchronized (this) {
			dataStore.store(writableAdapter);
			dataStore.store(index);

			ensureOpen(writableAdapter);
			entryInfo = HBaseUtils.write(
					writableAdapter,
					index,
					entry,
					writer);

		}
		return entryInfo.getRowIds();
	}

	private synchronized <T> void ensureOpen(final WritableDataAdapter<T> writableAdapter) {
		if (writer == null) {
			try {
				writer = operations.createWriter(StringUtils.stringFromBinary(index.getId().getBytes()), writableAdapter.getAdapterId().getString());
			}
			catch (final IOException e) {
				LOGGER.error(
						"Unable to open writer",
						e);
			}
		}
	}

	@Override
	public Index getIndex() {
		return index;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * mil.nga.giat.geowave.core.store.IndexWriter#setupAdapter(mil.nga.giat
	 * .geowave.core.store.adapter.WritableDataAdapter)
	 */
	@Override
	public <T> void setupAdapter(
			WritableDataAdapter<T> writableAdapter ) {
		LOGGER.error("This method is not yet coded. Need to fix it");

	}

}
