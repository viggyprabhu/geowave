package mil.nga.giat.geowave.adapter.raster.resize;

import java.io.IOException;

import mil.nga.giat.geowave.adapter.raster.RasterHelper;
import mil.nga.giat.geowave.adapter.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.adapter.raster.adapter.merge.nodata.NoDataMergeStrategy;
import mil.nga.giat.geowave.core.iface.store.StoreOperations;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.mapreduce.GeoWaveCoreConfiguratorBase;
import mil.nga.giat.geowave.core.store.mapreduce.GeoWaveCoreInputKey;
import mil.nga.giat.geowave.core.store.mapreduce.GeoWaveCoreOutputKey;
import mil.nga.giat.geowave.core.store.mapreduce.hadoop.GeoWaveCoreInputFormat;
import mil.nga.giat.geowave.core.store.mapreduce.hadoop.GeoWaveCoreOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.opengis.coverage.grid.GridCoverage;

public class RasterTileResizeJobRunner extends
		Configured implements
		Tool
{
	private static final Logger LOGGER = Logger.getLogger(RasterTileResizeJobRunner.class);

	public static final String NEW_ADAPTER_ID_KEY = "NEW_ADAPTER_ID";
	public static final String OLD_ADAPTER_ID_KEY = "OLD_ADAPTER_ID";

	protected String user;
	protected String password;
	protected String instance;
	protected String zookeeper;

	protected String oldNamespace;
	protected String oldCoverageName;
	protected String newNamespace;
	protected String newCoverageName;

	protected int minSplits;
	protected int maxSplits;
	protected int newTileSize;

	protected String hdfsHostPort;
	protected String jobTrackerOrResourceManHostPort;
	protected String indexId;

	public RasterTileResizeJobRunner() {

	}

	/**
	 * Main method to execute the MapReduce analytic.
	 */
	public int runJob()
			throws Exception {
		final Configuration conf = super.getConf();
		GeoWaveCoreConfiguratorBase.setRemoteInvocationParams(
				hdfsHostPort,
				jobTrackerOrResourceManHostPort,
				conf);
		conf.set(
				OLD_ADAPTER_ID_KEY,
				oldCoverageName);
		conf.set(
				NEW_ADAPTER_ID_KEY,
				newCoverageName);
		final Job job = new Job(
				conf);

		job.setJarByClass(this.getClass());

		job.setJobName("Converting " + oldCoverageName + " to tile size=" + newTileSize);

		job.setMapperClass(RasterTileResizeMapper.class);
		job.setCombinerClass(RasterTileResizeCombiner.class);
		job.setReducerClass(RasterTileResizeReducer.class);
		job.setInputFormatClass(GeoWaveCoreInputFormat.class);
		job.setOutputFormatClass(GeoWaveCoreOutputFormat.class);
		job.setMapOutputKeyClass(GeoWaveCoreInputKey.class);
		job.setMapOutputValueClass(ObjectWritable.class);
		job.setOutputKeyClass(GeoWaveCoreOutputKey.class);
		job.setOutputValueClass(GridCoverage.class);
		job.setNumReduceTasks(8);

		GeoWaveCoreInputFormat.setMinimumSplitCount(
				job.getConfiguration(),
				minSplits);
		GeoWaveCoreInputFormat.setMaximumSplitCount(
				job.getConfiguration(),
				maxSplits);
		GeoWaveCoreInputFormat.setAccumuloOperationsInfo(
				job,
				zookeeper,
				instance,
				user,
				password,
				oldNamespace);

		GeoWaveCoreOutputFormat.setAccumuloOperationsInfo(
				job,
				zookeeper,
				instance,
				user,
				password,
				newNamespace);
		final StoreOperations oldNamespaceOperations = RasterHelper.getStoreOperations(
				zookeeper,
				instance,
				user,
				password,
				oldNamespace);
		final DataAdapter adapter = RasterHelper.getAdapterStore(
				oldNamespaceOperations).getAdapter(
				new ByteArrayId(
						oldCoverageName));
		if (adapter == null) {
			throw new IllegalArgumentException(
					"Adapter for coverage '" + oldCoverageName + "' does not exist in namespace '" + oldNamespace + "'");
		}

		final RasterDataAdapter newAdapter = new RasterDataAdapter(
				(RasterDataAdapter) adapter,
				newCoverageName,
				newTileSize,
				new NoDataMergeStrategy());
		RasterHelper.getJobContextAdapterStore().addDataAdapter(
				job.getConfiguration(),
				adapter);
		RasterHelper.getJobContextAdapterStore().addDataAdapter(
				job.getConfiguration(),
				newAdapter);
		Index index = null;
		if (indexId != null) {
			index = RasterHelper.getIndexStore(
					oldNamespaceOperations).getIndex(
					new ByteArrayId(
							indexId));
		}
		if (index == null) {
			try (CloseableIterator<Index> indices = RasterHelper.getIndexStore(
					oldNamespaceOperations).getIndices()) {
				index = indices.next();
			}
			if (index == null) {
				throw new IllegalArgumentException(
						"Index does not exist in namespace '" + oldNamespaceOperations + "'");
			}
		}
		RasterHelper.getJobContextIndexStore().addIndex(
				job.getConfiguration(),
				index);
		final StoreOperations ops = RasterHelper.getStoreOperations(
				zookeeper,
				instance,
				user,
				password,
				newNamespace);
		final DataStore store = RasterHelper.getDataStore(ops);
		final IndexWriter writer = store.createIndexWriter(index);
		writer.setupAdapter(newAdapter);
		boolean retVal = false;
		try {
			retVal = job.waitForCompletion(true);
		}
		catch (IOException ex) {
			LOGGER.error(
					"Error waiting for map reduce tile resize job: ",
					ex);
		}
		finally {
			writer.close();
		}
		return retVal ? 0 : 1;
	}

	public static void main(
			final String[] args )
			throws Exception {
		final int res = ToolRunner.run(
				new Configuration(),
				new RasterTileResizeJobRunner(),
				args);
		System.exit(res);
	}

	@Override
	public int run(
			final String[] args )
			throws Exception {
		if (args.length > 0) {
			zookeeper = args[0];
			instance = args[1];
			user = args[2];
			password = args[3];
			oldNamespace = args[4];
			oldCoverageName = args[5];
			minSplits = Integer.parseInt(args[6]);
			maxSplits = Integer.parseInt(args[7]);
			hdfsHostPort = args[8];
			if (!hdfsHostPort.contains("://")) {
				hdfsHostPort = "hdfs://" + hdfsHostPort;
			}
			jobTrackerOrResourceManHostPort = args[9];
			newCoverageName = args[10];
			newNamespace = args[11];
			newTileSize = Integer.parseInt(args[12]);
			if (args.length > 13) {
				indexId = args[13];
			}
		}
		return runJob();
	}

}