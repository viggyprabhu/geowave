package mil.nga.giat.geowave.examples.mapreduce.dedupe;

import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GeoWaveDedupeJobRunner extends
		Configured implements
		Tool
{
	protected String user;
	protected String password;
	protected String instance;
	protected String zookeeper;
	protected String namespace;

	/**
	 * Main method to execute the MapReduce analytic.
	 */
	@SuppressWarnings("deprecation")
	public int runJob()
			throws Exception {
		final Configuration conf = super.getConf();
		final Job job = new Job(
				conf);

		GeoWaveInputFormat.setAccumuloOperationsInfo(
				job,
				zookeeper,
				instance,
				user,
				password,
				namespace);
		job.setJarByClass(this.getClass());

		job.setJobName("GeoWave Dedupe (" + namespace + ")");

		job.setMapperClass(GeoWaveDedupeMapper.class);
		job.setCombinerClass(GeoWaveDedupeCombiner.class);
		job.setReducerClass(GeoWaveDedupeReducer.class);
		job.setMapOutputKeyClass(GeoWaveInputKey.class);
		job.setMapOutputValueClass(Writable.class);
		job.setOutputKeyClass(GeoWaveInputKey.class);
		job.setOutputValueClass(Writable.class);

		job.setInputFormatClass(GeoWaveInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setNumReduceTasks(8);

		final FileSystem fs = FileSystem.get(conf);
		fs.delete(
				new Path(
						"/tmp/" + namespace + "_dedupe"),
				true);
		FileOutputFormat.setOutputPath(
				job,
				new Path(
						"/tmp/" + namespace + "_dedupe"));

		final boolean jobSuccess = job.waitForCompletion(true);

		return (jobSuccess) ? 0 : 1;
	}

	public static void main(
			final String[] args )
			throws Exception {
		final int res = ToolRunner.run(
				new Configuration(),
				new GeoWaveDedupeJobRunner(),
				args);
		System.exit(res);
	}

	@Override
	public int run(
			final String[] args )
			throws Exception {
		zookeeper = args[0];
		instance = args[1];
		user = args[2];
		password = args[3];
		namespace = args[4];
		return runJob();
	}
}
