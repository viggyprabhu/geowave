/**
 * 
 */
package mil.nga.giat.geowave.core.store.mapreduce.hadoop;

import java.io.IOException;

import mil.nga.giat.geowave.core.store.mapreduce.GeoWaveCoreOutputKey;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * @author viggy
 *
 */
public class GeoWaveCoreOutputFormat extends OutputFormat<GeoWaveCoreOutputKey, Object>{

	public static void setAccumuloOperationsInfo(Job job, String zookeeper,
			String instance, String user, String password, String newNamespace) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public RecordWriter<GeoWaveCoreOutputKey, Object> getRecordWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

}
