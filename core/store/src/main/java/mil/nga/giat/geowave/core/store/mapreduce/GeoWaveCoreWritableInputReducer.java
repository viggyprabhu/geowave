/**
 * 
 */
package mil.nga.giat.geowave.core.store.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.opengis.coverage.grid.GridCoverage;

/**
 * @author viggy
 * 
 */
public class GeoWaveCoreWritableInputReducer<KEYOUT, VALUEOUT> extends
		Reducer<GeoWaveCoreInputKey, ObjectWritable, KEYOUT, VALUEOUT>
{

	protected void reduceNativeValues(
			GeoWaveCoreInputKey key,
			Iterable<Object> values,
			Reducer<GeoWaveCoreInputKey, ObjectWritable, GeoWaveCoreOutputKey, GridCoverage>.Context context )
			throws IOException,
			InterruptedException {
		// TODO Need to fix this

	}

	@Override
	protected void setup(
			final Reducer<GeoWaveCoreInputKey, ObjectWritable, KEYOUT, VALUEOUT>.Context context )
			throws IOException,
			InterruptedException {
		// TODO Need to fix this

	}

}
