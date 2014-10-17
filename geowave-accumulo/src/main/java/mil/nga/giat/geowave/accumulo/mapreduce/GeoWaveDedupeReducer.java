package mil.nga.giat.geowave.accumulo.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class GeoWaveDedupeReducer extends
		Reducer<GeoWaveInputKey, Writable, GeoWaveInputKey, Writable>
{

	@Override
	protected void reduce(
			final GeoWaveInputKey key,
			final Iterable<Writable> values,
			final Reducer<GeoWaveInputKey, Writable, GeoWaveInputKey, Writable>.Context context )
			throws IOException,
			InterruptedException {
		final Iterator<Writable> objects = values.iterator();
		if (objects.hasNext()) {
			context.write(
					key,
					objects.next());
		}
	}

}
