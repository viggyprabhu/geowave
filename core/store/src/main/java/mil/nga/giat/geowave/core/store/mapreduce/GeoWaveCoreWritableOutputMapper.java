/**
 * 
 */
package mil.nga.giat.geowave.core.store.mapreduce;

import java.io.IOException;

import mil.nga.giat.geowave.core.store.mapreduce.hadoop.HadoopWritableSerializationTool;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.opengis.coverage.grid.GridCoverage;

/**
 * @author viggy
 * 
 */
public class GeoWaveCoreWritableOutputMapper<KEYIN, VALUEIN> extends
		Mapper<KEYIN, VALUEIN, GeoWaveCoreInputKey, ObjectWritable>
{

	protected HadoopWritableSerializationTool serializationTool;

	protected void mapNativeValue(
			GeoWaveCoreInputKey key,
			GridCoverage value,
			MapContext<GeoWaveCoreInputKey, GridCoverage, GeoWaveCoreInputKey, Object> context )
			throws IOException,
			InterruptedException {
		// TODO Need to fix this to work as GeoWaveWritableOutputMapper in
		// datastore-accumulo

	}

	@Override
	protected void setup(
			final Mapper<KEYIN, VALUEIN, GeoWaveCoreInputKey, ObjectWritable>.Context context )
			throws IOException,
			InterruptedException {
		// TODO Need to fix this to work as GeoWaveWritableOutputMapper in
		// datastore-accumulo
	}

}
