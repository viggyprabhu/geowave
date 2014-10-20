package mil.nga.giat.geowave.accumulo.mapreduce.dedupe;

import java.io.IOException;
import java.util.Iterator;

import mil.nga.giat.geowave.accumulo.mapreduce.HadoopDataAdapter;
import mil.nga.giat.geowave.accumulo.mapreduce.JobContextAdapterStore;
import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.adapter.DataAdapter;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class GeoWaveDedupeCombiner extends
		Reducer<GeoWaveInputKey, Object, GeoWaveInputKey, Writable>
{
	protected static final Logger LOGGER = Logger.getLogger(GeoWaveDedupeCombiner.class);
	protected AdapterStore adapterStore;

	@Override
	protected void reduce(
			final GeoWaveInputKey key,
			final Iterable<Object> values,
			final Reducer<GeoWaveInputKey, Object, GeoWaveInputKey, Writable>.Context context )
			throws IOException,
			InterruptedException {
		if (adapterStore != null) {
			final Iterator<Object> objects = values.iterator();
			if (objects.hasNext()) {
				final DataAdapter<?> adapter = adapterStore.getAdapter(key.getAdapterId());
				if (adapter instanceof HadoopDataAdapter) {
					context.write(
							key,
							((HadoopDataAdapter) adapter).toWritable(objects.next()));
				}
			}
		}
	}

	@Override
	protected void setup(
			final Reducer<GeoWaveInputKey, Object, GeoWaveInputKey, Writable>.Context context )
			throws IOException,
			InterruptedException {
		try {
			adapterStore = new JobContextAdapterStore(
					context,
					GeoWaveInputFormat.getAccumuloOperations(context));
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			LOGGER.warn(
					"Unable to get GeoWave adapter store from job context",
					e);
		}
	}
}
