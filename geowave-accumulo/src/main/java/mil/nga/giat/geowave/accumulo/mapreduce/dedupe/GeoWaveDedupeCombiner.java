package mil.nga.giat.geowave.accumulo.mapreduce.dedupe;

import java.io.IOException;
import java.util.Iterator;

import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputKey;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class GeoWaveDedupeCombiner extends
		Reducer<GeoWaveInputKey, Writable, GeoWaveInputKey, Writable>
{

	@Override
	protected void reduce(
			final GeoWaveInputKey key,
			final Iterable<Writable> values,
			final Reducer<GeoWaveInputKey, Writable, GeoWaveInputKey, Writable>.Context context )
			throws IOException,
			InterruptedException {
		final Iterator<Writable> it = values.iterator();
		while (it.hasNext()) {
			final Writable next = it.next();
			if (next != null) {
				context.write(
						key,
						next);
				return;
			}
		}
	}
}
