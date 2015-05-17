/**
 * 
 */
package mil.nga.giat.geowave.core.store.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author viggy
 *
 */
public class GeoWaveCoreReducer extends
Reducer<GeoWaveCoreInputKey, ObjectWritable, GeoWaveCoreInputKey, ObjectWritable>{

	protected void reduceNativeValues(
			GeoWaveCoreInputKey key,
			Iterable<Object> values,
			ReduceContext<GeoWaveCoreInputKey, ObjectWritable, GeoWaveCoreInputKey, Object> context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

}
