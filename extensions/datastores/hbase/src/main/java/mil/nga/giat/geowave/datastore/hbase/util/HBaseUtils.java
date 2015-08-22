/**
 * 
 */
package mil.nga.giat.geowave.datastore.hbase.util;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.ScanCallback;
import mil.nga.giat.geowave.core.store.adapter.AdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.data.DataWriter;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.data.visibility.UnconstrainedVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.visibility.UniformVisibilityWriter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.hbase.entities.HBaseRowId;
import mil.nga.giat.geowave.datastore.hbase.io.HBaseWriter;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.log4j.Logger;

/**
 * @author viggy Functionality similar to <code> AccumuloUtils </code>
 */
public class HBaseUtils
{

	private final static Logger LOGGER = Logger.getLogger(HBaseUtils.class);
	public static final String ALT_INDEX_TABLE = "_GEOWAVE_ALT_INDEX";

	private static final byte[] BEG_AND_BYTE = "&".getBytes(StringUtils.UTF8_CHAR_SET);
	private static final byte[] END_AND_BYTE = ")".getBytes(StringUtils.UTF8_CHAR_SET);

	private static final UniformVisibilityWriter DEFAULT_VISIBILITY = new UniformVisibilityWriter(
			new UnconstrainedVisibilityHandler());

	private static byte[] merge(
			final byte vis1[],
			final byte vis2[] ) {
		if ((vis1 == null) || (vis1.length == 0)) {
			return vis2;
		}
		else if ((vis2 == null) || (vis2.length == 0)) {
			return vis1;
		}

		final ByteBuffer buffer = ByteBuffer.allocate(vis1.length + 3 + vis2.length);
		buffer.putChar('(');
		buffer.put(vis1);
		buffer.putChar(')');
		buffer.put(BEG_AND_BYTE);
		buffer.put(vis2);
		buffer.put(END_AND_BYTE);
		return buffer.array();
	}

	public static <T> List<RowMutations> entryToMutations(
			final WritableDataAdapter<T> dataWriter,
			final Index index,
			final T entry,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {
		final DataStoreEntryInfo ingestInfo = getIngestInfo(
				dataWriter,
				index,
				entry,
				customFieldVisibilityWriter);
		return buildMutations(
				dataWriter.getAdapterId().getBytes(),
				ingestInfo);
	}

	private static List<RowMutations> buildMutations(
			final byte[] adapterId,
			final DataStoreEntryInfo ingestInfo ) {
		final List<RowMutations> mutations = new ArrayList<RowMutations>();
		final List<FieldInfo> fieldInfoList = ingestInfo.getFieldInfo();
		for (final ByteArrayId rowId : ingestInfo.getRowIds()) {
			final RowMutations mutation = new RowMutations(
					rowId.getBytes());

			try {
				Put row = new Put(
						rowId.getBytes());
				for (final FieldInfo fieldInfo : fieldInfoList) {
					row.addColumn(
							adapterId,
							fieldInfo.getDataValue().getId().getBytes(),
							fieldInfo.getWrittenValue());
				}
				mutation.add(row);
			}
			catch (IOException e) {
				LOGGER.warn("Could not add row to mutation.");
			}
			mutations.add(mutation);
		}
		return mutations;
	}

	@SuppressWarnings({
		"rawtypes",
		"unchecked"
	})
	public static <T> DataStoreEntryInfo getIngestInfo(
			final WritableDataAdapter<T> dataWriter,
			final Index index,
			final T entry,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {
		final CommonIndexModel indexModel;
		indexModel = index.getIndexModel();

		final AdapterPersistenceEncoding encodedData = dataWriter.encode(
				entry,
				indexModel);
		final List<ByteArrayId> insertionIds = encodedData.getInsertionIds(index);
		final List<ByteArrayId> rowIds = new ArrayList<ByteArrayId>(
				insertionIds.size());
		final PersistentDataset extendedData = encodedData.getAdapterExtendedData();
		final PersistentDataset indexedData = encodedData.getCommonData();
		final List<PersistentValue> extendedValues = extendedData.getValues();
		final List<PersistentValue> commonValues = indexedData.getValues();

		final List<FieldInfo> fieldInfoList = new ArrayList<FieldInfo>();

		if (!insertionIds.isEmpty()) {
			addToRowIds(
					rowIds,
					insertionIds,
					dataWriter.getDataId(
							entry).getBytes(),
					dataWriter.getAdapterId().getBytes(),
					encodedData.isDeduplicationEnabled());

			for (final PersistentValue fieldValue : commonValues) {
				final FieldInfo<T> fieldInfo = getFieldInfo(
						indexModel,
						fieldValue,
						entry,
						customFieldVisibilityWriter);
				if (fieldInfo != null) {
					fieldInfoList.add(fieldInfo);
				}
			}
			for (final PersistentValue fieldValue : extendedValues) {
				if (fieldValue.getValue() != null) {
					final FieldInfo<T> fieldInfo = getFieldInfo(
							dataWriter,
							fieldValue,
							entry,
							customFieldVisibilityWriter);
					if (fieldInfo != null) {
						fieldInfoList.add(fieldInfo);
					}
				}
			}
			return new DataStoreEntryInfo(
					rowIds,
					fieldInfoList);
		}
		LOGGER.warn("Indexing failed to produce insertion ids; entry [" + dataWriter.getDataId(
				entry).getString() + "] not saved.");
		return new DataStoreEntryInfo(
				Collections.EMPTY_LIST,
				Collections.EMPTY_LIST);

	}

	private static <T> void addToRowIds(
			final List<ByteArrayId> rowIds,
			final List<ByteArrayId> insertionIds,
			final byte[] dataId,
			final byte[] adapterId,
			final boolean enableDeduplication ) {

		final int numberOfDuplicates = insertionIds.size() - 1;

		for (final ByteArrayId insertionId : insertionIds) {
			final byte[] indexId = insertionId.getBytes();
			// because the combination of the adapter ID and data ID
			// gaurantees uniqueness, we combine them in the row ID to
			// disambiguate index values that are the same, also adding
			// enough length values to be able to read the row ID again, we
			// lastly add a number of duplicates which can be useful as
			// metadata in our de-duplication
			// step
			rowIds.add(new ByteArrayId(
					new HBaseRowId(
							indexId,
							dataId,
							adapterId,
							enableDeduplication ? numberOfDuplicates : -1).getRowId()));
		}
	}

	@SuppressWarnings({
		"rawtypes",
		"unchecked"
	})
	private static <T> FieldInfo<T> getFieldInfo(
			final DataWriter dataWriter,
			final PersistentValue<T> fieldValue,
			final T entry,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {
		final FieldWriter fieldWriter = dataWriter.getWriter(fieldValue.getId());
		final FieldVisibilityHandler<T, Object> customVisibilityHandler = customFieldVisibilityWriter.getFieldVisibilityHandler(fieldValue.getId());
		if (fieldWriter != null) {
			final Object value = fieldValue.getValue();
			return new FieldInfo<T>(
					fieldValue,
					fieldWriter.writeField(value),
					merge(
							customVisibilityHandler.getVisibility(
									entry,
									fieldValue.getId(),
									value),
							fieldWriter.getVisibility(
									entry,
									fieldValue.getId(),
									value)));
		}
		else if (fieldValue.getValue() != null) {
			LOGGER.warn("Data writer of class " + dataWriter.getClass() + " does not support field for " + fieldValue.getValue());
		}
		return null;
	}

	public static <T> DataStoreEntryInfo write(
			final WritableDataAdapter<T> writableAdapter,
			final Index index,
			final T entry,
			final HBaseWriter writer,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {
		final DataStoreEntryInfo ingestInfo = getIngestInfo(
				writableAdapter,
				index,
				entry,
				customFieldVisibilityWriter);
		final List<RowMutations> mutations = buildMutations(
				writableAdapter.getAdapterId().getBytes(),
				ingestInfo);

		try {
			writer.write(
					mutations,
					writableAdapter.getAdapterId().getString());
		}
		catch (IOException e) {
			LOGGER.warn("Writing to table failed." + e);
		}
		return ingestInfo;
	}

	public static String getQualifiedTableName(
			final String tableNamespace,
			final String unqualifiedTableName ) {
		return ((tableNamespace == null) || tableNamespace.isEmpty()) ? unqualifiedTableName : tableNamespace + "_" + unqualifiedTableName;
	}

	public static <T> DataStoreEntryInfo write(
			final WritableDataAdapter<T> writableAdapter,
			final Index index,
			final T entry,
			final HBaseWriter writer ) {
		return write(
				writableAdapter,
				index,
				entry,
				writer,
				DEFAULT_VISIBILITY);
	}

	@SuppressWarnings({
		"rawtypes",
		"unchecked"
	})
	private static <T> FieldInfo<T> getFieldInfo(
			final PersistentValue<T> fieldValue,
			final byte[] value,
			final byte[] visibility ) {
		return new FieldInfo<T>(
				fieldValue,
				value,
				visibility);
	}

	public static List<ByteArrayRange> constraintsToByteArrayRanges(
			final MultiDimensionalNumericData constraints,
			final NumericIndexStrategy indexStrategy,
			final int maxRanges ) {
		if ((constraints == null) || constraints.isEmpty()) {
			return new ArrayList<ByteArrayRange>(); // implies in negative and
			// positive infinity
		}
		else {
			return indexStrategy.getQueryRanges(
					constraints,
					maxRanges);
		}
	}

	@SuppressWarnings("unchecked")
	public static <T> T decodeRow(
			Result row,
			final AdapterStore adapterStore,
			final QueryFilter clientFilter,
			final Index index,
			final ScanCallback<T> scanCallback ) {

		final HBaseRowId rowId = new HBaseRowId(
				row.getRow());
		return (T) decodeRowObj(
				row,
				rowId,
				null,
				adapterStore,
				clientFilter,
				index,
				scanCallback);
	}

	private static <T> Object decodeRowObj(
			final Result row,
			final HBaseRowId rowId,
			final DataAdapter<T> dataAdapter,
			final AdapterStore adapterStore,
			final QueryFilter clientFilter,
			final Index index,
			final ScanCallback<T> scanCallback ) {
		final Pair<T, DataStoreEntryInfo> pair = decodeRow(
				row,
				rowId,
				dataAdapter,
				adapterStore,
				clientFilter,
				index,
				scanCallback);
		return pair != null ? pair.getLeft() : null;

	}

	@SuppressWarnings("unchecked")
	public static <T> Pair<T, DataStoreEntryInfo> decodeRow(
			final Result row,
			final HBaseRowId rowId,
			final DataAdapter<T> dataAdapter,
			final AdapterStore adapterStore,
			final QueryFilter clientFilter,
			final Index index,
			final ScanCallback<T> scanCallback ) {
		if ((dataAdapter == null) && (adapterStore == null)) {
			LOGGER.error("Could not decode row from iterator. Either adapter or adapter store must be non-null.");
			return null;
		}
		DataAdapter<T> adapter = dataAdapter;
		List<KeyValue> rowMapping;
		try {
			rowMapping = getSortedRowMapping(row);
		}
		catch (final IOException e) {
			LOGGER.error("Could not decode row from iterator. Ensure whole row iterators are being used.");
			return null;
		}
		// build a persistence encoding object first, pass it through the
		// client filters and if its accepted, use the data adapter to
		// decode the persistence model into the native data type
		final PersistentDataset<CommonIndexValue> indexData = new PersistentDataset<CommonIndexValue>();
		final PersistentDataset<Object> extendedData = new PersistentDataset<Object>();
		// for now we are assuming all entries in a row are of the same type
		// and use the same adapter
		boolean adapterMatchVerified;
		ByteArrayId adapterId;
		if (adapter != null) {
			adapterId = adapter.getAdapterId();
			adapterMatchVerified = false;
		}
		else {
			adapterMatchVerified = true;
			adapterId = null;
		}

		final List<FieldInfo> fieldInfoList = new ArrayList<FieldInfo>(
				rowMapping.size());

		for (final KeyValue entry : rowMapping) {
			// the column family is the data element's type ID
			if (adapterId == null) {
				adapterId = new ByteArrayId(
						entry.getFamily());
				// entry.getKey().getColumnFamilyData().getBackingArray());
			}

			if (adapter == null) {
				adapter = (DataAdapter<T>) adapterStore.getAdapter(adapterId);
				if (adapter == null) {
					LOGGER.error("DataAdapter does not exist");
					return null;
				}
			}
			if (!adapterMatchVerified) {
				if (!adapterId.equals(adapter.getAdapterId())) {
					return null;
				}
				adapterMatchVerified = true;
			}
			final ByteArrayId fieldId = new ByteArrayId(
					entry.getQualifier());
			// entry.getKey().getColumnQualifierData().getBackingArray());
			final CommonIndexModel indexModel;
			indexModel = index.getIndexModel();

			// first check if this field is part of the index model
			final FieldReader<? extends CommonIndexValue> indexFieldReader = indexModel.getReader(fieldId);
			final byte byteValue[] = entry.getValue();
			if (indexFieldReader != null) {
				final CommonIndexValue indexValue = indexFieldReader.readField(byteValue);
				// indexValue.setVisibility(entry.getKey().getColumnVisibilityData().getBackingArray());
				final PersistentValue<CommonIndexValue> val = new PersistentValue<CommonIndexValue>(
						fieldId,
						indexValue);
				indexData.addValue(val);
				fieldInfoList.add(getFieldInfo(
						val,
						byteValue,
						indexValue.getVisibility()));
			}
			else {
				// next check if this field is part of the adapter's
				// extended data model
				final FieldReader<?> extFieldReader = adapter.getReader(fieldId);
				if (extFieldReader == null) {
					// if it still isn't resolved, log an error, and
					// continue
					LOGGER.error("field reader not found for data entry, the value will be ignored");
					continue;
				}
				final Object value = extFieldReader.readField(byteValue);
				final PersistentValue<Object> val = new PersistentValue<Object>(
						fieldId,
						value);
				extendedData.addValue(val);
				fieldInfoList.add(getFieldInfo(
						val,
						byteValue,
						null));
				// entry.getKey().getColumnVisibility().getBytes()));
			}
		}

		final IndexedAdapterPersistenceEncoding encodedRow = new IndexedAdapterPersistenceEncoding(
				adapterId,
				new ByteArrayId(
						rowId.getDataId()),
				new ByteArrayId(
						rowId.getInsertionId()),
				rowId.getNumberOfDuplicates(),
				indexData,
				extendedData);

		if ((clientFilter == null) || clientFilter.accept(encodedRow)) {
			// cannot get here unless adapter is found (not null)
			if (adapter == null) {
				LOGGER.error("Error, adapter was null when it should not be");
			}
			else {
				final Pair<T, DataStoreEntryInfo> pair = Pair.of(
						adapter.decode(
								encodedRow,
								index),
						new DataStoreEntryInfo(
								Arrays.asList(new ByteArrayId(
										row.getRow())),
								// getRowData().getBackingArray())),
								fieldInfoList));
				if (scanCallback != null) {
					scanCallback.entryScanned(
							pair.getRight(),
							pair.getLeft());
				}
				return pair;
			}
		}
		return null;
	}

	private static List<KeyValue> getSortedRowMapping(
			Result row )
			throws IOException {
		List<KeyValue> map = new ArrayList<KeyValue>();
		/*
		 * ByteArrayInputStream in = new
		 * ByteArrayInputStream(v.getValueArray()); DataInputStream din = new
		 * DataInputStream( in); int numKeys = din.readInt(); for (int i = 0; i
		 * < numKeys; i++) { byte[] cf = readField(din); // read the col fam
		 * byte[] cq = readField(din); // read the col qual byte[] cv =
		 * readField(din); // read the col visibility long timestamp =
		 * din.readLong(); // read the timestamp byte[] valBytes =
		 * readField(din); // read the value map.add(new KeyValue(
		 * CellUtil.cloneRow(v), cf, cq, timestamp, valBytes));
		 */
		NavigableMap<byte[], NavigableMap<byte[], byte[]>> noVersionMap = row.getNoVersionMap();
		for (byte[] family : noVersionMap.keySet()) {
			for (byte[] qualifier : noVersionMap.get(
					family).keySet()) {
				Cell cell = CellUtil.createCell(
						row.getRow(),
						family,
						qualifier,
						System.currentTimeMillis(),
						KeyValue.Type.Put.getCode(),
						noVersionMap.get(
								family).get(
								qualifier));
				map.add(new KeyValue(
						cell));
			}
		}
		return map;
	}

	private static byte[] readField(
			DataInputStream din )
			throws IOException {
		int len = din.readInt();
		byte[] b = new byte[len];
		int readLen = din.read(b);
		if (len > 0 && len != readLen) {
			throw new IOException(
					String.format(
							"Expected to read %d bytes but read %d",
							len,
							readLen));
		}
		return b;
	}

	public static <T> void writeAltIndex(
			final WritableDataAdapter<T> writableAdapter,
			final DataStoreEntryInfo entryInfo,
			final T entry,
			final HBaseWriter writer ) {

		final byte[] adapterId = writableAdapter.getAdapterId().getBytes();
		final byte[] dataId = writableAdapter.getDataId(
				entry).getBytes();
		if ((dataId != null) && (dataId.length > 0)) {
			final List<RowMutations> mutations = new ArrayList<RowMutations>();

			for (final ByteArrayId rowId : entryInfo.getRowIds()) {
				final RowMutations mutation = new RowMutations(
						rowId.getBytes());

				try {
					Put row = new Put(
							rowId.getBytes());
					row.addColumn(
							adapterId,
							rowId.getBytes(),
							"".getBytes(StringUtils.UTF8_CHAR_SET));
					mutation.add(row);
				}
				catch (IOException e) {
					LOGGER.warn("Could not add row to mutation.");
				}
				mutations.add(mutation);
			}
			try {
				writer.write(
						mutations,
						writableAdapter.getAdapterId().getString());
			}
			catch (IOException e) {
				LOGGER.warn("Writing to table failed." + e);
			}

		}
	}

	public static RowMutations getDeleteMutations(
			byte[] rowId,
			byte[] columnFamily,
			byte[] columnQualifier,
			String[] authorizations )
			throws IOException {
		RowMutations m = new RowMutations(
				rowId);
		Delete d = new Delete(
				rowId);
		d.addColumn(
				columnFamily,
				columnQualifier);
		m.add(d);
		return m;
	}

}