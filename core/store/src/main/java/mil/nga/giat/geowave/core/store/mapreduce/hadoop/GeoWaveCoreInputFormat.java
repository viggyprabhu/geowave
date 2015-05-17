/**
 * 
 */
package mil.nga.giat.geowave.core.store.mapreduce.hadoop;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

import mil.nga.giat.geowave.core.store.mapreduce.GeoWaveCoreConfiguratorBase;
import mil.nga.giat.geowave.core.store.mapreduce.GeoWaveCoreInputKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

/**
 * @author viggy
 *
 */
public class GeoWaveCoreInputFormat<T> extends InputFormat<GeoWaveCoreInputKey, T>{

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RecordReader<GeoWaveCoreInputKey, T> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}
	
	private static final Class<?> CLASS = GeoWaveCoreInputFormat.class;
	protected static final Logger LOGGER = Logger.getLogger(CLASS);
	private static final BigInteger TWO = BigInteger.valueOf(2L);

	/**
	 * Configures a {@link AccumuloOperations} for this job.
	 * 
	 * @param config
	 *            the Hadoop configuration instance
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
			final Configuration config,
			final String zooKeepers,
			final String instanceName,
			final String userName,
			final String password,
			final String geowaveTableNamespace ) {
		GeoWaveCoreConfiguratorBase.setAccumuloOperationsInfo(
				CLASS,
				config,
				zooKeepers,
				instanceName,
				userName,
				password,
				geowaveTableNamespace);
	}

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
		setAccumuloOperationsInfo(
				job.getConfiguration(),
				zooKeepers,
				instanceName,
				userName,
				password,
				geowaveTableNamespace);
	}

	public static void setMinimumSplitCount(
			final Configuration config,
			final Integer minSplits ) {
		GeoWaveCoreInputConfigurator.setMinimumSplitCount(
				CLASS,
				config,
				minSplits);
	}

	public static void setMaximumSplitCount(
			final Configuration config,
			final Integer maxSplits ) {
		GeoWaveCoreInputConfigurator.setMaximumSplitCount(
				CLASS,
				config,
				maxSplits);
	}
}
