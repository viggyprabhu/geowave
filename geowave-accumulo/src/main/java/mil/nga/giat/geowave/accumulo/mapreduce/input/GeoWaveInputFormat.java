package mil.nga.giat.geowave.accumulo.mapreduce.input;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.accumulo.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.accumulo.mapreduce.JobContextAdapterStore;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.accumulo.query.AccumuloRangeQuery;
import mil.nga.giat.geowave.accumulo.util.AccumuloUtils;
import mil.nga.giat.geowave.accumulo.util.CloseableIteratorWrapper;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.NumericIndexStrategy;
import mil.nga.giat.geowave.index.PersistenceUtils;
import mil.nga.giat.geowave.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.query.DistributableQuery;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.TabletLocator;
import org.apache.accumulo.core.client.mapreduce.lib.util.ConfiguratorBase;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;

public class GeoWaveInputFormat extends
		InputFormat<GeoWaveInputKey, Object>
{
	private static final Class<?> CLASS = GeoWaveInputFormat.class;
	protected static final Logger LOGGER = Logger.getLogger(CLASS);

	/**
	 * Configures a {@link AccumuloOperations} for this job.
	 *
	 * @param job
	 *            the Hadoop job instance to be configured
	 * @param zooKeepers
	 *            a comma-separated list of zookeeper servers
	 * @param instanceName
	 *            the Accumulo instance name
	 * @param userName
	 *            the Accumulo user name
	 * @param password
	 *            the Accumulo password
	 * @param geowaveTableNamespace
	 *            the GeoWave table namespace
	 */
	public static void setAccumuloOperationsInfo(
			final Job job,
			final String zooKeepers,
			final String instanceName,
			final String userName,
			final String password,
			final String geowaveTableNamespace ) {
		GeoWaveConfiguratorBase.setZookeeperUrl(
				CLASS,
				job,
				zooKeepers);
		GeoWaveConfiguratorBase.setInstanceName(
				CLASS,
				job,
				instanceName);
		GeoWaveConfiguratorBase.setUserName(
				CLASS,
				job,
				userName);
		GeoWaveConfiguratorBase.setPassword(
				CLASS,
				job,
				password);
		GeoWaveConfiguratorBase.setTableNamespace(
				CLASS,
				job,
				geowaveTableNamespace);
	}

	public static void addDataAdapter(
			final Job job,
			final DataAdapter<?> adapter ) {
		GeoWaveConfiguratorBase.addDataAdapter(
				CLASS,
				job,
				adapter);
	}

	public static void setMinimumSplitCount(
			final Job job,
			final Integer minSplits ) {
		GeoWaveInputConfigurator.setMinimumSplitCount(
				CLASS,
				job,
				minSplits);
	}

	public static void setMaximumSplitCount(
			final Job job,
			final Integer maxSplits ) {
		GeoWaveInputConfigurator.setMaximumSplitCount(
				CLASS,
				job,
				maxSplits);
	}

	public static void setQuery(
			final Job job,
			final DistributableQuery query ) {
		GeoWaveInputConfigurator.setQuery(
				CLASS,
				job,
				query);
	}

	protected static DistributableQuery getQuery(
			final JobContext context ) {
		return GeoWaveInputConfigurator.getQuery(
				CLASS,
				context);
	}

	protected static Index getIndex(
			final JobContext context,
			final ByteArrayId indexId ) {
		return GeoWaveConfiguratorBase.getIndex(
				CLASS,
				context,
				indexId);
	}

	protected static Index[] getIndices(
			final JobContext context ) {
		final Index[] userIndices = GeoWaveConfiguratorBase.getIndices(
				CLASS,
				context);
		if ((userIndices == null) || (userIndices.length <= 0)) {
			try {
				// if there are no indices, assume we are searching all indices
				// in the metadata store
				return (Index[]) IteratorUtils.toArray(
						new AccumuloIndexStore(
								getAccumuloOperations(context)).getIndices(),
						Index.class);
			}
			catch (AccumuloException | AccumuloSecurityException e) {
				LOGGER.warn(
						"Unable to lookup indices from GeoWave metadata store",
						e);
			}
		}
		return userIndices;
	}

	protected static String getTableNamespace(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getTableNamespace(
				CLASS,
				context);
	}

	protected static String getUserName(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getUserName(
				CLASS,
				context);
	}

	protected static String getPassword(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getPassword(
				CLASS,
				context);
	}

	/**
	 * Initializes an Accumulo {@link TabletLocator} based on the configuration.
	 *
	 * @param instance
	 *            the accumulo instance
	 * @param tableName
	 *            the accumulo table name
	 * @return an Accumulo tablet locator
	 * @throws TableNotFoundException
	 *             if the table name set on the configuration doesn't exist
	 * @since 1.5.0
	 */
	protected static TabletLocator getTabletLocator(
			final Instance instance,
			final String tableName )
			throws TableNotFoundException {
		return TabletLocator.getInstance(
				instance,
				new Text(
						Tables.getTableId(
								instance,
								tableName)));

	}

	protected static String getInstanceName(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getInstanceName(
				CLASS,
				context);
	}

	protected static Integer getMinimumSplitCount(
			final JobContext context ) {
		return GeoWaveInputConfigurator.getMinimumSplitCount(
				CLASS,
				context);
	}

	protected static Integer getMaximumSplitCount(
			final JobContext context ) {
		return GeoWaveInputConfigurator.getMaximumSplitCount(
				CLASS,
				context);
	}

	protected static Instance getInstance(
			final JobContext context ) {
		return GeoWaveInputConfigurator.getInstance(
				CLASS,
				context);
	}

	/**
	 * Read the metadata table to get tablets and match up ranges to them.
	 */
	@Override
	public List<InputSplit> getSplits(
			final JobContext context )
			throws IOException,
			InterruptedException {
		LOGGER.setLevel(getLogLevel(context));
		validateOptions(context);
		final Index[] indices = getIndices(context);
		final List<InputSplit> splits = new ArrayList<InputSplit>();
		final DistributableQuery query = getQuery(context);
		final String tableNamespace = getTableNamespace(context);
		for (final Index index : indices) {
			if ((query != null) && !query.isSupported(index)) {
				continue;
			}
			final String tableName = AccumuloUtils.getQualifiedTableName(
					tableNamespace,
					index.getId().getString());
			final NumericIndexStrategy indexStrategy = index.getIndexStrategy();
			final TreeSet<Range> ranges;
			//TODO set the ranges to at least min splits
			if (query != null) {
				final MultiDimensionalNumericData indexConstraints = query.getIndexConstraints(indexStrategy);
				ranges = AccumuloUtils.byteArrayRangesToAccumuloRanges(AccumuloUtils.constraintsToByteArrayRanges(
						indexConstraints,
						indexStrategy));
			}
			else {
				ranges = new TreeSet<Range>();
				ranges.add(new Range());
			}

			// get the metadata information for these ranges
			final Map<String, Map<KeyExtent, List<Range>>> binnedRanges = new HashMap<String, Map<KeyExtent, List<Range>>>();
			TabletLocator tl;
			try {
				final Instance instance = getInstance(context);
				tl = getTabletLocator(
						instance,
						tableName);
				// its possible that the cache could contain complete, but
				// old information about a tables tablets... so clear it
				tl.invalidateCache();
				final String instanceId = instance.getInstanceID();
				final ByteArrayOutputStream backingByteArray = new ByteArrayOutputStream();
				final DataOutputStream output = new DataOutputStream(
						backingByteArray);
				new PasswordToken(
						getPassword(context)).write(output);
				output.close();
				final ByteBuffer buffer = ByteBuffer.wrap(backingByteArray.toByteArray());
				final TCredentials credentials = new TCredentials(
						getUserName(context),
						PasswordToken.class.getCanonicalName(),
						buffer,
						instanceId);
				String tableId = null;
				final List<Range> rangeList = new ArrayList<Range>(
						ranges);
				while (!tl.binRanges(
						rangeList,
						binnedRanges,
						credentials).isEmpty()) {
					if (!(instance instanceof MockInstance)) {
						if (tableId == null) {
							tableId = Tables.getTableId(
									instance,
									tableName);
						}
						if (!Tables.exists(
								instance,
								tableId)) {
							throw new TableDeletedException(
									tableId);
						}
						if (Tables.getTableState(
								instance,
								tableId) == TableState.OFFLINE) {
							throw new TableOfflineException(
									instance,
									tableId);
						}
					}
					binnedRanges.clear();
					LOGGER.warn("Unable to locate bins for specified ranges. Retrying.");
					UtilWaitThread.sleep(100 + (int) (Math.random() * 100));
					// sleep randomly between 100 and 200 ms
					tl.invalidateCache();
				}
			}
			catch (final Exception e) {
				throw new IOException(
						e);
			}

			final HashMap<String, String> hostNameCache = new HashMap<String, String>();

			for (final Entry<String, Map<KeyExtent, List<Range>>> tserverBin : binnedRanges.entrySet()) {
				final String ip = tserverBin.getKey().split(
						":",
						2)[0];
				String location = hostNameCache.get(ip);
				if (location == null) {
					final InetAddress inetAddress = InetAddress.getByName(ip);
					location = inetAddress.getHostName();
					hostNameCache.put(
							ip,
							location);
				}

				for (final Entry<KeyExtent, List<Range>> extentRanges : tserverBin.getValue().entrySet()) {
					final Range ke = extentRanges.getKey().toDataRange();
					for (final Range r : extentRanges.getValue()) {
						//TODO merge splits on the same tablet to fit within max splits
//						splits.add(new RangeInputSplit(
//								index,
//								ke.clip(r),
//								new String[] {
//									location
//								}));
					}
				}
			}
		}
		return splits;
	}

	@Override
	public RecordReader<GeoWaveInputKey, Object> createRecordReader(
			final InputSplit split,
			final TaskAttemptContext context )
			throws IOException,
			InterruptedException {
		LOGGER.setLevel(getLogLevel(context));
		return new GeoWaveRecordReader<Object>();
	}

	public static void addIndex(
			final Job job,
			final Index index ) {
		GeoWaveConfiguratorBase.addIndex(
				CLASS,
				job,
				index);
	}

	/**
	 * Sets the log level for this job.
	 *
	 * @param job
	 *            the Hadoop job instance to be configured
	 * @param level
	 *            the logging level
	 * @since 1.5.0
	 */
	public static void setLogLevel(
			final Job job,
			final Level level ) {
		ConfiguratorBase.setLogLevel(
				CLASS,
				job.getConfiguration(),
				level);
	}

	/**
	 * Gets the log level from this configuration.
	 *
	 * @param context
	 *            the Hadoop context for the configured job
	 * @return the log level
	 * @since 1.5.0
	 * @see #setLogLevel(Job, Level)
	 */
	protected static Level getLogLevel(
			final JobContext context ) {
		return ConfiguratorBase.getLogLevel(
				CLASS,
				GeoWaveConfiguratorBase.getConfiguration(context));
	}

	/**
	 * Check whether a configuration is fully configured to be used with an
	 * Accumulo {@link org.apache.hadoop.mapreduce.InputFormat}.
	 *
	 * @param context
	 *            the Hadoop context for the configured job
	 * @throws IOException
	 *             if the context is improperly configured
	 * @since 1.5.0
	 */
	protected static void validateOptions(
			final JobContext context )
			throws IOException {
		// the only required element is the AccumuloOperations info
		try {
			// this should attempt to use the connection info to successfully
			// connect
			if (getAccumuloOperations(context) == null) {
				LOGGER.warn("Zookeeper connection for accumulo is null");
				throw new IOException(
						"Zookeeper connection for accumulo is null");
			}
		}
		catch (final AccumuloException e) {
			LOGGER.warn(
					"Error establishing zookeeper connection for accumulo",
					e);
			throw new IOException(
					e);
		}
		catch (final AccumuloSecurityException e) {
			LOGGER.warn(
					"Security error while establishing connection to accumulo",
					e);
			throw new IOException(
					e);
		}
	}

	public static AccumuloOperations getAccumuloOperations(
			final JobContext context )
			throws AccumuloException,
			AccumuloSecurityException {
		return GeoWaveConfiguratorBase.getAccumuloOperations(
				CLASS,
				context);
	}

	protected static String[] getAuthorizations(
			final JobContext context )
			throws AccumuloException,
			AccumuloSecurityException {
		return GeoWaveInputConfigurator.getAuthorizations(
				CLASS,
				context);
	}

	protected static JobContextAdapterStore getDataAdapterStore(
			final JobContext context,
			final AccumuloOperations accumuloOperations ) {
		return new JobContextAdapterStore(
				context,
				accumuloOperations);
	}

	/**
	 * The Class RangeInputSplit. Encapsulates an Accumulo range for use in Map
	 * Reduce jobs.
	 */
	public static class RangeInputSplit extends
			InputSplit implements
			Writable
	{
		private Index index;
		private Range[] ranges;
		private String[] locations;

		public RangeInputSplit() {
			ranges = new Range[] {
				new Range()
			};
			locations = new String[0];
		}

		public RangeInputSplit(
				final RangeInputSplit split )
				throws IOException {
			setRanges(split.getRanges());
			setLocations(split.getLocations());
		}

		protected RangeInputSplit(
				final Index index,
				final Range[] range,
				final String[] locations ) {
			this.index = index;
			ranges = ranges;
			this.locations = locations;
		}

		public Range[] getRanges() {
			return ranges;
		}

		public void setRanges(
				final Range[] ranges ) {
			this.ranges = ranges;
		}

		public Index getIndex() {
			return index;
		}

		public void setIndex(
				final Index index ) {
			this.index = index;
		}

		private static byte[] extractBytes(
				final ByteSequence seq,
				final int numBytes ) {
			final byte[] bytes = new byte[numBytes + 1];
			bytes[0] = 0;
			for (int i = 0; i < numBytes; i++) {
				if (i >= seq.length()) {
					bytes[i + 1] = 0;
				}
				else {
					bytes[i + 1] = seq.byteAt(i);
				}
			}
			return bytes;
		}

		public static float getProgress(
				final ByteSequence start,
				final ByteSequence end,
				final ByteSequence position ) {
			final int maxDepth = Math.min(
					Math.max(
							end.length(),
							start.length()),
					position.length());
			final BigInteger startBI = new BigInteger(
					extractBytes(
							start,
							maxDepth));
			final BigInteger endBI = new BigInteger(
					extractBytes(
							end,
							maxDepth));
			final BigInteger positionBI = new BigInteger(
					extractBytes(
							position,
							maxDepth));
			return (float) (positionBI.subtract(
					startBI).doubleValue() / endBI.subtract(
					startBI).doubleValue());
		}

		public float getProgress(
				Range range,
				final Key currentKey ) {
//			if (currentKey == null) {
//				return 0f;
//			}
//			if ((range.getStartKey() != null) && (range.getEndKey() != null)) {
//				if (!range.getStartKey().equals(
//						range.getEndKey(),
//						PartialKey.ROW)) {
//					// just look at the row progress
//					return getProgress(
//							range.getStartKey().getRowData(),
//							range.getEndKey().getRowData(),
//							currentKey.getRowData());
//				}
//				else if (!range.getStartKey().equals(
//						range.getEndKey(),
//						PartialKey.ROW_COLFAM)) {
//					// just look at the column family progress
//					return getProgress(
//							range.getStartKey().getColumnFamilyData(),
//							range.getEndKey().getColumnFamilyData(),
//							currentKey.getColumnFamilyData());
//				}
//				else if (!range.getStartKey().equals(
//						range.getEndKey(),
//						PartialKey.ROW_COLFAM_COLQUAL)) {
//					// just look at the column qualifier progress
//					return getProgress(
//							range.getStartKey().getColumnQualifierData(),
//							range.getEndKey().getColumnQualifierData(),
//							currentKey.getColumnQualifierData());
//				}
//			}
			// if we can't figure it out, then claim no progress
			return 0f;
		}

		/**
		 * This implementation of length is only an estimate, it does not
		 * provide exact values. Do not have your code rely on this return
		 * value.
		 */
		@Override
		public long getLength()
				throws IOException {
//			final Text startRow = range.isInfiniteStartKey() ? new Text(
//					new byte[] {
//						Byte.MIN_VALUE
//					}) : range.getStartKey().getRow();
//			final Text stopRow = range.isInfiniteStopKey() ? new Text(
//					new byte[] {
//						Byte.MAX_VALUE
//					}) : range.getEndKey().getRow();
//			final int maxCommon = Math.min(
//					7,
//					Math.min(
//							startRow.getLength(),
//							stopRow.getLength()));
//			long diff = 0;
//
//			final byte[] start = startRow.getBytes();
//			final byte[] stop = stopRow.getBytes();
//			for (int i = 0; i < maxCommon; ++i) {
//				diff |= 0xff & (start[i] ^ stop[i]);
//				diff <<= Byte.SIZE;
//			}
//
//			if (startRow.getLength() != stopRow.getLength()) {
//				diff |= 0xff;
//			}
//
//			return diff + 1;
			return 0;
		}

		@Override
		public String[] getLocations()
				throws IOException {
			return locations;
		}

		public void setLocations(
				final String[] locations ) {
			this.locations = locations;
		}

		@Override
		public void readFields(
				final DataInput in )
				throws IOException {
			final int numRanges = in.readInt();
			final Range[] ranges = new Range[numRanges];
			for (int i = 0; i < numRanges; i++) {
				try {
					ranges[i] = Range.class.newInstance();
					ranges[i].readFields(in);
				}
				catch (InstantiationException | IllegalAccessException e) {
					throw new IOException(
							"Unable to instantiate range",
							e);
				}
			}
			final int numLocs = in.readInt();
			locations = new String[numLocs];
			for (int i = 0; i < numLocs; ++i) {
				locations[i] = in.readUTF();
			}

			final int indexLength = in.readInt();
			final byte[] indexBytes = new byte[indexLength];
			in.readFully(indexBytes);
			index = PersistenceUtils.fromBinary(
					indexBytes,
					Index.class);
		}

		@Override
		public void write(
				final DataOutput out )
				throws IOException {
			out.writeInt(ranges.length);
			for (final Range range : ranges) {
				range.write(out);
			}
			out.writeInt(locations.length);
			for (final String location : locations) {
				out.writeUTF(location);
			}
			final byte[] indexBytes = PersistenceUtils.toBinary(index);
			out.writeInt(indexBytes.length);
			out.write(indexBytes);
		}
	}

	protected static class GeoWaveRecordReader<T> extends
			RecordReader<GeoWaveInputKey, T>
	{
		protected long numKeysRead;
		protected CloseableIterator<?> iterator;
		protected Key currentAccumuloKey = null;
		protected GeoWaveInputKey currentGeoWaveKey = null;
		protected T currentValue = null;
		protected RangeInputSplit split;

		/**
		 * Initialize a scanner over the given input split using this task
		 * attempt configuration.
		 */
		@Override
		public void initialize(
				final InputSplit inSplit,
				final TaskAttemptContext attempt )
				throws IOException {
			split = (RangeInputSplit) inSplit;

			try {
				final AccumuloOperations operations = getAccumuloOperations(attempt);

				final JobContextAdapterStore adapterStore = getDataAdapterStore(
						attempt,
						operations);
				final DistributableQuery query = getQuery(attempt);
				final String[] additionalAuthorizations = getAuthorizations(attempt);

				numKeysRead = 0;
				final List<CloseableIterator<?>> iterators = new ArrayList<CloseableIterator<?>>();
				for (final Range r : split.ranges) {
					iterators.add(new AccumuloRangeQuery(
							adapterStore.getAdapterIds(),
							split.index,
							r,
							query.createFilters(split.index.getIndexModel()),
							additionalAuthorizations).query(
							operations,
							adapterStore,
							null));
				}
				// concatenate iterators
				iterator = new CloseableIteratorWrapper<Object>(
						new Closeable() {
							@Override
							public void close()
									throws IOException {
								for (final CloseableIterator<?> it : iterators) {
									it.close();
								}
							}
						},
						Iterators.concat(iterators.iterator()));
			}
			catch (AccumuloException | AccumuloSecurityException e) {
				LOGGER.error(
						"Unable to query accumulo for range input split",
						e);
			}
		}

		@Override
		public void close() {
			if (iterator != null) {
				try {
					iterator.close();
				}
				catch (final IOException e) {
					LOGGER.warn(
							"Unable to close iterator",
							e);
				}
			}
		}

		@Override
		public boolean nextKeyValue()
				throws IOException,
				InterruptedException {
			if (iterator != null) {
				if (iterator.hasNext()) {
					++numKeysRead;
					final Object value = iterator.next();
					if (value instanceof Entry) {
						final Entry<Key, T> entry = (Entry<Key, T>) value;
						currentAccumuloKey = entry.getKey();
						if (currentAccumuloKey == null) {
							currentGeoWaveKey = null;
						}
						else {
							currentGeoWaveKey = AccumuloUtils.accumuloKeyToGeoWaveKey(currentAccumuloKey);
						}
						currentValue = entry.getValue();
					}
					return true;
				}
			}
			return false;
		}

		@Override
		public float getProgress()
				throws IOException {
			if ((numKeysRead > 0) && (currentAccumuloKey == null)) {
				return 1.0f;
			}
			return 0;
//			return split.getProgress(currentAccumuloKey);
		}

		@Override
		public GeoWaveInputKey getCurrentKey()
				throws IOException,
				InterruptedException {
			return currentGeoWaveKey;
		}

		@Override
		public T getCurrentValue()
				throws IOException,
				InterruptedException {
			return currentValue;
		}
	}
}