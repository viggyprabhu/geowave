package mil.nga.giat.geowave.index.dimension;

import mil.nga.giat.geowave.index.dimension.bin.BinningStrategy;
import mil.nga.giat.geowave.index.dimension.bin.TemporalBinningStrategy;
import mil.nga.giat.geowave.index.dimension.bin.TemporalBinningStrategy.Unit;
import mil.nga.giat.geowave.index.sfc.data.NumericData;
import mil.nga.giat.geowave.index.sfc.data.NumericRange;

/**
 * The Time Definition class is a convenience class used to define a dimension
 * which is associated with a time dimension.
 * 
 */
public class TimeDefinition extends
		UnboundedDimensionDefinition
{
	protected TimeDefinition() {
		super();
	}

	/**
	 * Constructor used to create a new Unbounded Binning Strategy based upon a
	 * temporal binning strategy of the unit parameter. The unit can be of DAY,
	 * MONTH, or YEAR.
	 * 
	 * @param unit
	 *            an enumeration of temporal units (DAY, MONTH, or YEAR)
	 */
	public TimeDefinition(
			final Unit unit ) {
		super(
				new TemporalBinningStrategy(
						unit));

	}

	/**
	 * Constructor used to create a new Unbounded Binning Strategy based upon a
	 * generic binning strategy.
	 * 
	 * @param binningStrategy
	 *            a object which defines the bins
	 */
	public TimeDefinition(
			final BinningStrategy binningStrategy ) {
		super(
				binningStrategy);
	}

	@Override
	public NumericData getFullRange() {
		return new NumericRange(
				0,
				System.currentTimeMillis() + 1);
	}
}
