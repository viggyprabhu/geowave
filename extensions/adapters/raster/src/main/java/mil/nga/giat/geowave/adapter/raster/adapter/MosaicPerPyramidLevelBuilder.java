/**
 * 
 */
package mil.nga.giat.geowave.adapter.raster.adapter;

import java.awt.Rectangle;
import java.awt.geom.AffineTransform;
import java.awt.geom.NoninvertibleTransformException;
import java.awt.image.DataBuffer;
import java.util.Iterator;

import javax.media.jai.Interpolation;

import mil.nga.giat.geowave.adapter.raster.FitToIndexGridCoverage;
import mil.nga.giat.geowave.adapter.raster.Resolution;
import mil.nga.giat.geowave.adapter.raster.plugin.GeoWaveGTRasterFormat;
import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.core.iface.store.Converter;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.HierarchicalNumericIndexStrategy.SubStrategy;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;

import org.apache.log4j.Logger;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.geometry.GeneralEnvelope;
import org.geotools.geometry.jts.GeometryClipper;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.geotools.renderer.lite.RendererUtilities;
import org.opengis.coverage.grid.GridCoverage;
import org.opengis.geometry.Envelope;
import org.opengis.referencing.datum.PixelInCell;
import org.opengis.referencing.operation.TransformException;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

/**
 * @author viggy
 *
 */
public class MosaicPerPyramidLevelBuilder implements
Converter<SubStrategy, GridCoverage>
{
	private final static Logger LOGGER = Logger.getLogger(MosaicPerPyramidLevelBuilder.class);
	private final MultiDimensionalNumericData originalBounds;
	private final GridCoverage originalData;
	private final int tileSize;
	private final double[] backgroundValuesPerBand;
	private final Geometry footprint;
	private final Interpolation defaultInterpolation;

	public MosaicPerPyramidLevelBuilder(
			final MultiDimensionalNumericData originalBounds,
			final GridCoverage originalData,
			final int tileSize,
			final double[] backgroundValuesPerBand,
			final Geometry footprint,
			final Interpolation defaultInterpolation ) {
		this.originalBounds = originalBounds;
		this.originalData = originalData;
		this.tileSize = tileSize;
		this.backgroundValuesPerBand = backgroundValuesPerBand;
		this.footprint = footprint;
		this.defaultInterpolation = defaultInterpolation;
	}

	@Override
	public Iterator<GridCoverage> convert(
			final SubStrategy pyramidLevel ) {
		final Iterator<ByteArrayId> insertionIds = pyramidLevel.getIndexStrategy().getInsertionIds(
				originalBounds).iterator();
		return new Iterator<GridCoverage>() {

			@Override
			public boolean hasNext() {
				return insertionIds.hasNext();
			}

			@Override
			public GridCoverage next() {
				final ByteArrayId insertionId = insertionIds.next();
				if (insertionId == null) {
					return null;
				}
				final MultiDimensionalNumericData rangePerDimension = pyramidLevel.getIndexStrategy().getRangeForId(
						insertionId);
				final NumericDimensionDefinition[] dimensions = pyramidLevel.getIndexStrategy().getOrderedDimensionDefinitions();
				int longitudeIndex = 0, latitudeIndex = 1;
				final double[] minDP = new double[2];
				final double[] maxDP = new double[2];
				for (int d = 0; d < dimensions.length; d++) {
					if (dimensions[d] instanceof LatitudeDefinition) {
						latitudeIndex = d;
						minDP[1] = originalBounds.getMinValuesPerDimension()[d];
						maxDP[1] = originalBounds.getMaxValuesPerDimension()[d];
					}
					else if (dimensions[d] instanceof LongitudeDefinition) {
						longitudeIndex = d;
						minDP[0] = originalBounds.getMinValuesPerDimension()[d];
						maxDP[0] = originalBounds.getMaxValuesPerDimension()[d];
					}
				}

				final Envelope originalEnvelope = new GeneralEnvelope(
						minDP,
						maxDP);
				final double[] minsPerDimension = rangePerDimension.getMinValuesPerDimension();
				final double[] maxesPerDimension = rangePerDimension.getMaxValuesPerDimension();
				final ReferencedEnvelope mapExtent = new ReferencedEnvelope(
						minsPerDimension[longitudeIndex],
						maxesPerDimension[longitudeIndex],
						minsPerDimension[latitudeIndex],
						maxesPerDimension[latitudeIndex],
						GeoWaveGTRasterFormat.DEFAULT_CRS);
				final AffineTransform worldToScreenTransform = RendererUtilities.worldToScreenTransform(
						mapExtent,
						new Rectangle(
								tileSize,
								tileSize));
				GridGeometry2D insertionIdGeometry;
				try {
					final AffineTransform2D gridToCRS = new AffineTransform2D(
							worldToScreenTransform.createInverse());
					insertionIdGeometry = new GridGeometry2D(
							new GridEnvelope2D(
									new Rectangle(
											tileSize,
											tileSize)),
											PixelInCell.CELL_CORNER,
											gridToCRS,
											GeoWaveGTRasterFormat.DEFAULT_CRS,
											null);

					final double[] tileRes = pyramidLevel.getIndexStrategy().getHighestPrecisionIdRangePerDimension();
					final double[] pixelRes = new double[tileRes.length];
					for (int d = 0; d < tileRes.length; d++) {
						pixelRes[d] = tileRes[d] / tileSize;
					}
					Geometry footprintWithinTileWorldGeom = null;
					Geometry footprintWithinTileScreenGeom = null;
					try {
						final Geometry wholeFootprintScreenGeom = JTS.transform(
								footprint,
								new AffineTransform2D(
										worldToScreenTransform));
						final com.vividsolutions.jts.geom.Envelope fullTileEnvelope = new com.vividsolutions.jts.geom.Envelope(
								0,
								tileSize,
								0,
								tileSize);
						final GeometryClipper tileClipper = new GeometryClipper(
								fullTileEnvelope);
						footprintWithinTileScreenGeom = tileClipper.clip(
								wholeFootprintScreenGeom,
								true);
						if (footprintWithinTileScreenGeom == null) {
							// for some reason the original image footprint
							// falls outside this insertion ID
							LOGGER.warn("Original footprint geometry (" + originalData.getGridGeometry() + ") falls outside the insertion bounds (" + insertionIdGeometry + ")");
							return null;
						}
						footprintWithinTileWorldGeom = JTS.transform(
								footprintWithinTileScreenGeom,
								gridToCRS);
						if (footprintWithinTileScreenGeom.covers(new GeometryFactory().toGeometry(fullTileEnvelope))) {
							// if the screen geometry fully covers the tile,
							// don't bother carrying it forward
							footprintWithinTileScreenGeom = null;
						}
					}
					catch (final TransformException e) {
						LOGGER.warn(
								"Unable to calculate geometry of footprint for tile",
								e);
					}

					Interpolation tileInterpolation = defaultInterpolation;
					final int dataType = originalData.getRenderedImage().getSampleModel().getDataType();

					// TODO a JAI bug "workaround" in GeoTools does not
					// work, this is a workaround for the GeoTools bug
					// see https://jira.codehaus.org/browse/GEOT-3585, and
					// line 666-698 of
					// org.geotools.coverage.processing.operation.Resampler2D
					// (gt-coverage-12.1)
					if ((dataType == DataBuffer.TYPE_FLOAT) || (dataType == DataBuffer.TYPE_DOUBLE)) {
						final Envelope tileEnvelope = insertionIdGeometry.getEnvelope();
						final ReferencedEnvelope tileReferencedEnvelope = new ReferencedEnvelope(
								new com.vividsolutions.jts.geom.Envelope(
										tileEnvelope.getMinimum(0),
										tileEnvelope.getMaximum(0),
										tileEnvelope.getMinimum(1),
										tileEnvelope.getMaximum(1)),
										GeoWaveGTRasterFormat.DEFAULT_CRS);
						final Geometry tileJTSGeometry = new GeometryFactory().toGeometry(tileReferencedEnvelope);
						if (!footprint.contains(tileJTSGeometry)) {
							tileInterpolation = Interpolation.getInstance(Interpolation.INTERP_NEAREST);
						}
					}
					final GridCoverage resampledCoverage = (GridCoverage) RasterDataAdapter.getResampleOperations().resample(
							originalData,
							GeoWaveGTRasterFormat.DEFAULT_CRS,
							insertionIdGeometry,
							tileInterpolation,
							backgroundValuesPerBand);
					// NOTE: for now this is commented out, but beware the
					// resample operation under certain conditions,
					// this requires more investigation rather than adding a
					// hacky fix

					// sometimes the resample results in an image that is
					// not tileSize in width and height although the
					// insertionIdGeometry is telling it to resample to
					// tileSize

					// in these cases, check and perform a rescale to
					// finalize the grid coverage to guarantee it is the
					// correct tileSize

					// final GridEnvelope e =
					// resampledCoverage.getGridGeometry().getGridRange();
					// boolean resize = false;

					// for (int d = 0; d < e.getDimension(); d++) {
					// if (e.getSpan(d) != tileSize) {
					// resize = true;
					// break;
					// }
					// }
					// if (resize) {
					// resampledCoverage = Operations.DEFAULT.scale(
					// resampledCoverage,
					// (double) tileSize / (double) e.getSpan(0),
					// (double) tileSize / (double) e.getSpan(1),
					// -resampledCoverage.getRenderedImage().getMinX(),
					// -resampledCoverage.getRenderedImage().getMinY());
					// }
					// if ((resampledCoverage.getRenderedImage().getWidth()
					// != tileSize) ||
					// (resampledCoverage.getRenderedImage().getHeight() !=
					// tileSize) ||
					// (resampledCoverage.getRenderedImage().getMinX() != 0)
					// || (resampledCoverage.getRenderedImage().getMinY() !=
					// 0)) {
					// resampledCoverage = Operations.DEFAULT.scale(
					// resampledCoverage,
					// 1,
					// 1,
					// -resampledCoverage.getRenderedImage().getMinX(),
					// -resampledCoverage.getRenderedImage().getMinY());
					// }
					return new FitToIndexGridCoverage(
							resampledCoverage,
							insertionId,
							new Resolution(
									pixelRes),
									originalEnvelope,
									footprintWithinTileWorldGeom,
									footprintWithinTileScreenGeom);
				}
				catch (IllegalArgumentException | NoninvertibleTransformException e) {
					LOGGER.warn(
							"Unable to calculate transformation for grid coordinates on write",
							e);
				}
				return null;
			}

			@Override
			public void remove() {
				insertionIds.remove();
			}
		};
	}
}
