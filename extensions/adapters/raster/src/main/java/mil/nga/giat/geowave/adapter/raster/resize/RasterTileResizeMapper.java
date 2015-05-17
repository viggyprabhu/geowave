package mil.nga.giat.geowave.adapter.raster.resize;

import java.io.IOException;
import java.util.Iterator;

import mil.nga.giat.geowave.adapter.raster.FitToIndexGridCoverage;
import mil.nga.giat.geowave.adapter.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.mapreduce.GeoWaveCoreInputKey;
import mil.nga.giat.geowave.core.store.mapreduce.GeoWaveCoreWritableOutputMapper;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.opengis.coverage.grid.GridCoverage;

public class RasterTileResizeMapper extends
		GeoWaveCoreWritableOutputMapper<GeoWaveCoreInputKey, GridCoverage>
{
	protected static final Logger LOGGER = Logger.getLogger(RasterTileResizeMapper.class);
	private RasterTileResizeHelper helper;

	@Override
	protected void mapNativeValue(
			final GeoWaveCoreInputKey key,
			final GridCoverage value,
			final MapContext<GeoWaveCoreInputKey, GridCoverage, GeoWaveCoreInputKey, Object> context )
			throws IOException,
			InterruptedException {
		if (helper.isOriginalCoverage(key.getAdapterId())) {
			final DataAdapter<?> adapter = super.serializationTool.getAdapter(key.getAdapterId());
			if ((adapter != null) && (adapter instanceof RasterDataAdapter)) {
				final Iterator<GridCoverage> coverages = helper.getCoveragesForIndex(value);
				if (coverages == null) {
					LOGGER.error("Couldn't get coverages instance, getCoveragesForIndex returned null");
					throw new IOException(
							"Couldn't get coverages instance, getCoveragesForIndex returned null");
				}
				while (coverages.hasNext()) {
					final GridCoverage c = coverages.next();
					// it should be a FitToIndexGridCoverage because it was just
					// converted above
					if (c instanceof FitToIndexGridCoverage) {
						context.write(
								new GeoWaveCoreInputKey(
										helper.getNewCoverageId(),
										((FitToIndexGridCoverage) c).getInsertionId()),
								c);
					}
				}
			}
		}
	}

	@Override
	protected void setup(
			final Mapper<GeoWaveCoreInputKey, GridCoverage, GeoWaveCoreInputKey, ObjectWritable>.Context context )
			throws IOException,
			InterruptedException {
		super.setup(context);
		helper = new RasterTileResizeHelper(
				context);
	}

}
