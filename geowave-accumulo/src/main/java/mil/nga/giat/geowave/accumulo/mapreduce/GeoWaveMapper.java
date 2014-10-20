package mil.nga.giat.geowave.accumulo.mapreduce;

import java.io.IOException;

import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.adapter.DataAdapter;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public abstract class GeoWaveMapper extends
		Mapper<GeoWaveInputKey, Writable, GeoWaveInputKey, Writable>
{
	protected static final Logger LOGGER = Logger.getLogger(GeoWaveWritableInputMapper.class);
	protected AdapterStore adapterStore;

	@Override
	protected void map(
			final GeoWaveInputKey key,
			final Writable value,
			final Mapper<GeoWaveInputKey, Writable, GeoWaveInputKey, Writable>.Context context )
			throws IOException,
			InterruptedException {
		mapWritableValue(
				key,
				value,
				context);
	}

	protected void mapWritableValue(
			final GeoWaveInputKey key,
			final Writable value,
			final Mapper<GeoWaveInputKey, Writable, GeoWaveInputKey, Writable>.Context context )
			throws IOException,
			InterruptedException {
		if (adapterStore != null) {
			final DataAdapter<?> adapter = adapterStore.getAdapter(key.getAdapterId());
			if ((adapter != null) && (adapter instanceof HadoopDataAdapter)) {
				mapNativeValue(
						key,
						((HadoopDataAdapter) adapter).fromWritable(value),
						new NativeMapContext<GeoWaveInputKey, Writable>(
								context,
								adapterStore));
			}
		}
	}

	protected abstract void mapNativeValue(
			final GeoWaveInputKey key,
			final Object value,
			final MapContext<GeoWaveInputKey, Writable, GeoWaveInputKey, Object> context )
			throws IOException,
			InterruptedException;

	@Override
	protected void setup(
			final Mapper<GeoWaveInputKey, Writable, GeoWaveInputKey, Writable>.Context context )
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
