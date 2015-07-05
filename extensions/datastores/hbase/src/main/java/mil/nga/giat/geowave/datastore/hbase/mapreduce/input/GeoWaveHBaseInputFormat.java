/**
 * 
 */
package mil.nga.giat.geowave.datastore.hbase.mapreduce.input;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.datastore.hbase.mapreduce.GeoWaveHBaseConfiguratorBase;
import mil.nga.giat.geowave.datastore.hbase.mapreduce.JobContextHBaseAdapterStore;
import mil.nga.giat.geowave.datastore.hbase.mapreduce.JobContextHBaseIndexStore;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * @author viggy
 * 
 */
public class GeoWaveHBaseInputFormat<T> extends
		InputFormat<GeoWaveHBaseInputKey, T>
{

	private static final Class<?> CLASS = GeoWaveHBaseInputFormat.class;
	protected static final Logger LOGGER = Logger.getLogger(CLASS);
	private static final BigInteger TWO = BigInteger.valueOf(2L);

	@Override
	public List<InputSplit> getSplits(
			JobContext context )
			throws IOException,
			InterruptedException {
		// TODO #406 Need to fix
		/*
		 * LOGGER.setLevel(getLogLevel(context)); validateOptions(context);
		 * final Integer minSplits = getMinimumSplitCount(context); final
		 * Integer maxSplits = getMaximumSplitCount(context); final
		 * TreeSet<IntermediateSplitInfo> splits = getIntermediateSplits(
		 * context, maxSplits); // this is an incremental algorithm, it may be
		 * better use the target // split count to drive it (ie. to get 3 splits
		 * this will split 1 large // range into two down the middle and then
		 * split one of those ranges // down the middle to get 3, rather than
		 * splitting one range into // thirds) if ((minSplits != null) &&
		 * (splits.size() < minSplits)) { // set the ranges to at least min
		 * splits do { // remove the highest range, split it into 2 and add both
		 * back, // increasing the size by 1 final IntermediateSplitInfo
		 * highestSplit = splits.pollLast(); final IntermediateSplitInfo
		 * otherSplit = highestSplit.split(); splits.add(highestSplit);
		 * splits.add(otherSplit); } while (splits.size() < minSplits); } else
		 * if (((maxSplits != null) && (maxSplits > 0)) && (splits.size() >
		 * maxSplits)) { // merge splits to fit within max splits do { // this
		 * is the naive approach, remove the lowest two ranges and // merge
		 * them, decreasing the size by 1
		 * 
		 * // TODO Ideally merge takes into account locations (as well as //
		 * possibly the index as a secondary criteria) to limit the // number of
		 * locations/indices final IntermediateSplitInfo lowestSplit =
		 * splits.pollFirst(); final IntermediateSplitInfo nextLowestSplit =
		 * splits.pollFirst(); lowestSplit.merge(nextLowestSplit);
		 * splits.add(lowestSplit); } while (splits.size() > maxSplits); }
		 */
		final List<InputSplit> retVal = new ArrayList<InputSplit>();
		/*
		 * for (final IntermediateSplitInfo split : splits) {
		 * retVal.add(split.toFinalSplit()); }
		 */

		return retVal;

	}

	@Override
	public RecordReader<GeoWaveHBaseInputKey, T> createRecordReader(
			InputSplit split,
			TaskAttemptContext context )
			throws IOException,
			InterruptedException {
		// TODO #406 Need to fix
		LOGGER.error("This method createRecordReader2 is not yet coded. Need to fix it");
		return null;
	}

	public static void setOperationsInfo(
			Configuration config,
			String zooKeepers,
			String geowaveTableNamespace ) {
		GeoWaveHBaseConfiguratorBase.setOperationsInfo(
				CLASS,
				config,
				zooKeepers,
				geowaveTableNamespace);

	}

	public static void setOperationsInfo(
			final Job job,
			final String zooKeepers,
			final String geowaveTableNamespace ) {
		setOperationsInfo(
				job.getConfiguration(),
				zooKeepers,
				geowaveTableNamespace);
	}

	public static BasicHBaseOperations getOperations(
			final JobContext context )
			throws IOException {
		return GeoWaveHBaseConfiguratorBase.getOperations(
				CLASS,
				context);
	}

	public static void addDataAdapter(
			final Configuration config,
			final DataAdapter<?> adapter ) {

		// Also store for use the mapper and reducers
		JobContextHBaseAdapterStore.addDataAdapter(
				config,
				adapter);
		GeoWaveHBaseConfiguratorBase.addDataAdapter(
				CLASS,
				config,
				adapter);
	}

	public static void addIndex(
			final Configuration config,
			final Index index ) {
		JobContextHBaseIndexStore.addIndex(
				config,
				index);
	}

	public static void setQuery(
			final Configuration config,
			final DistributableQuery query ) {
		GeoWaveHBaseInputConfigurator.setQuery(
				CLASS,
				config,
				query);
	}

	public static void setMinimumSplitCount(
			final Configuration config,
			final Integer minSplits ) {
		GeoWaveHBaseInputConfigurator.setMinimumSplitCount(
				CLASS,
				config,
				minSplits);
	}

	public static void setMaximumSplitCount(
			final Configuration config,
			final Integer maxSplits ) {
		GeoWaveHBaseInputConfigurator.setMaximumSplitCount(
				CLASS,
				config,
				maxSplits);
	}

	protected static Level getLogLevel(
			final JobContext context ) {
		// TODO #406 Need to fix
		LOGGER.warn("This log level is currently hardcoded. Need to fix it later.");
		return Level.INFO;
	}

	protected static void validateOptions(
			final JobContext context )
			throws IOException {
		// this should attempt to use the connection info to successfully
		// connect
		if (getOperations(context) == null) {
			LOGGER.warn("Zookeeper connection for accumulo is null");
			throw new IOException(
					"Zookeeper connection for accumulo is null");
		}
	}

	protected static Integer getMinimumSplitCount(
			final JobContext context ) {
		return GeoWaveHBaseInputConfigurator.getMinimumSplitCount(
				CLASS,
				context);
	}

	protected static Integer getMaximumSplitCount(
			final JobContext context ) {
		return GeoWaveHBaseInputConfigurator.getMaximumSplitCount(
				CLASS,
				context);
	}

	/*
	 * private TreeSet<IntermediateSplitInfo> getIntermediateSplits( final
	 * JobContext context, final Integer maxSplits ) throws IOException { final
	 * Index[] indices = getIndices(context); final DistributableQuery query =
	 * getQuery(context); final String tableNamespace =
	 * getTableNamespace(context);
	 * 
	 * final TreeSet<IntermediateSplitInfo> splits = new
	 * TreeSet<IntermediateSplitInfo>(); for (final Index index : indices) { if
	 * ((query != null) && !query.isSupported(index)) { continue; } final String
	 * tableName = HBaseUtils.getQualifiedTableName( tableNamespace,
	 * index.getId().getString()); final NumericIndexStrategy indexStrategy =
	 * index.getIndexStrategy(); final TreeSet<Range> ranges; if (query != null)
	 * { final MultiDimensionalNumericData indexConstraints =
	 * query.getIndexConstraints(indexStrategy); if ((maxSplits != null) &&
	 * (maxSplits > 0)) { ranges =
	 * AccumuloUtils.byteArrayRangesToAccumuloRanges(
	 * AccumuloUtils.constraintsToByteArrayRanges( indexConstraints,
	 * indexStrategy, maxSplits)); } else { ranges =
	 * AccumuloUtils.byteArrayRangesToAccumuloRanges
	 * (AccumuloUtils.constraintsToByteArrayRanges( indexConstraints,
	 * indexStrategy)); } } else { ranges = new TreeSet<Range>(); ranges.add(new
	 * Range()); } // get the metadata information for these ranges final
	 * Map<String, Map<KeyExtent, List<Range>>> tserverBinnedRanges = new
	 * HashMap<String, Map<KeyExtent, List<Range>>>(); TabletLocator tl; try {
	 * final Instance instance = getInstance(context); final String tableId =
	 * Tables.getTableId( instance, tableName); tl = getTabletLocator( instance,
	 * tableName, tableId); // its possible that the cache could contain
	 * complete, but // old information about a tables tablets... so clear it
	 * tl.invalidateCache(); final String instanceId = instance.getInstanceID();
	 * final List<Range> rangeList = new ArrayList<Range>( ranges); Random r =
	 * new Random(); while (!binRanges( rangeList, getUserName(context),
	 * getPassword(context), tserverBinnedRanges, tl, instanceId)) {
	 * tserverBinnedRanges.clear();
	 * LOGGER.warn("Unable to locate bins for specified ranges. Retrying.");
	 * UtilWaitThread.sleep(100 + r.nextInt(101)); // sleep randomly between 100
	 * and 200 ms tl.invalidateCache(); } } catch (final Exception e) { throw
	 * new IOException( e); } final HashMap<String, String> hostNameCache = new
	 * HashMap<String, String>(); for (final Entry<String, Map<KeyExtent,
	 * List<Range>>> tserverBin : tserverBinnedRanges.entrySet()) { final String
	 * tabletServer = tserverBin.getKey(); final String ipAddress =
	 * tabletServer.split( ":", 2)[0];
	 * 
	 * String location = hostNameCache.get(ipAddress); if (location == null) {
	 * final InetAddress inetAddress = InetAddress.getByName(ipAddress);
	 * location = inetAddress.getHostName(); hostNameCache.put( ipAddress,
	 * location); } for (final Entry<KeyExtent, List<Range>> extentRanges :
	 * tserverBin.getValue().entrySet()) { final Range keyExtent =
	 * extentRanges.getKey().toDataRange(); final Map<Index,
	 * List<RangeLocationPair>> splitInfo = new HashMap<Index,
	 * List<RangeLocationPair>>(); final List<RangeLocationPair> rangeList = new
	 * ArrayList<RangeLocationPair>(); for (final Range range :
	 * extentRanges.getValue()) { rangeList.add(new RangeLocationPair(
	 * keyExtent.clip(range), location)); } splitInfo.put( index, rangeList);
	 * splits.add(new IntermediateSplitInfo( splitInfo)); } } } return splits;
	 * //TODO #406 Need to fix return null; }
	 */

}
