package mil.nga.giat.geowave.adapter.raster.resize;

import java.io.IOException;

import mil.nga.giat.geowave.core.store.mapreduce.GeoWaveCoreInputKey;
import mil.nga.giat.geowave.core.store.mapreduce.GeoWaveCoreOutputKey;
import mil.nga.giat.geowave.core.store.mapreduce.GeoWaveCoreWritableInputReducer;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.opengis.coverage.grid.GridCoverage;

public class RasterTileResizeReducer extends
		GeoWaveCoreWritableInputReducer<GeoWaveCoreOutputKey, GridCoverage>
{
	private RasterTileResizeHelper helper;

	@Override
	protected void reduceNativeValues(
			final GeoWaveCoreInputKey key,
			final Iterable<Object> values,
			final Reducer<GeoWaveCoreInputKey, ObjectWritable, GeoWaveCoreOutputKey, GridCoverage>.Context context )
			throws IOException,
			InterruptedException {
		final GridCoverage mergedCoverage = helper.getMergedCoverage(
				key,
				values);
		if (mergedCoverage != null) {
			context.write(
					helper.getGeoWaveOutputKey(),
					mergedCoverage);
		}
	}

	@Override
	protected void setup(
			final Reducer<GeoWaveCoreInputKey, ObjectWritable, GeoWaveCoreOutputKey, GridCoverage>.Context context )
			throws IOException,
			InterruptedException {
		super.setup(context);
		helper = new RasterTileResizeHelper(
				context);
	}

}
