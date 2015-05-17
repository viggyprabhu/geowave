package mil.nga.giat.geowave.adapter.raster.resize;

import java.io.IOException;

import mil.nga.giat.geowave.core.store.mapreduce.GeoWaveCoreInputKey;
import mil.nga.giat.geowave.core.store.mapreduce.GeoWaveCoreReducer;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.opengis.coverage.grid.GridCoverage;

public class RasterTileResizeCombiner extends
		GeoWaveCoreReducer
{
	private RasterTileResizeHelper helper;

	@Override
	protected void reduceNativeValues(
			final GeoWaveCoreInputKey key,
			final Iterable<Object> values,
			final ReduceContext<GeoWaveCoreInputKey, ObjectWritable, GeoWaveCoreInputKey, Object> context )
			throws IOException,
			InterruptedException {
		final GridCoverage mergedCoverage = helper.getMergedCoverage(
				key,
				values);
		if (mergedCoverage != null) {
			context.write(
					key,
					mergedCoverage);
		}

	}

	@Override
	protected void setup(
			final Reducer<GeoWaveCoreInputKey, ObjectWritable, GeoWaveCoreInputKey, ObjectWritable>.Context context )
			throws IOException,
			InterruptedException {
		super.setup(context);
		helper = new RasterTileResizeHelper(
				context);
	}

}
