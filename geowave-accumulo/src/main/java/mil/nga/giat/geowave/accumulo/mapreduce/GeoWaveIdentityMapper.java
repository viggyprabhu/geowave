package mil.nga.giat.geowave.accumulo.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;

public class GeoWaveIdentityMapper extends
		Mapper<GeoWaveInputKey, Object, GeoWaveInputKey, Object>
{

	@Override
	protected void map(
			final GeoWaveInputKey key,
			final Object value,
			final Mapper<GeoWaveInputKey, Object, GeoWaveInputKey, Object>.Context context )
			throws IOException,
			InterruptedException {
		context.write(
				key,
				value);
	}

}
