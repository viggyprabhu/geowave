package mil.nga.giat.geowave.adapter.raster;

import java.util.TreeMap;

import mil.nga.giat.geowave.core.index.HierarchicalNumericIndexStrategy;
import mil.nga.giat.geowave.core.index.HierarchicalNumericIndexStrategy.SubStrategy;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.CustomIdIndex;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.query.Query;

public class RasterDataStore 
{

	private DataStore m_dataStore;

	public RasterDataStore(
			final DataStore dataStore ) {
		m_dataStore = dataStore;
	}
	

	public <T> CloseableIterator<T> query(
			final DataAdapter<T> adapter,
			final Index index,
			final Query query,
			final double[] targetResolutionPerDimension ) {
		// determine the correct tier to query for the given resolution
		final NumericIndexStrategy strategy = index.getIndexStrategy();
		if (strategy instanceof HierarchicalNumericIndexStrategy) {
			final TreeMap<Double, SubStrategy> sortedStrategies = new TreeMap<Double, SubStrategy>();
			SubStrategy targetIndexStrategy = null;
			for (final SubStrategy subStrategy : ((HierarchicalNumericIndexStrategy) strategy).getSubStrategies()) {
				final double[] idRangePerDimension = subStrategy.getIndexStrategy().getHighestPrecisionIdRangePerDimension();
				double rangeSum = 0;
				for (final double range : idRangePerDimension) {
					rangeSum += range;
				}
				// sort by the sum of the range in each dimension
				sortedStrategies.put(
						rangeSum,
						subStrategy);
			}
			for (final SubStrategy subStrategy : sortedStrategies.descendingMap().values()) {
				final double[] highestPrecisionIdRangePerDimension = subStrategy.getIndexStrategy().getHighestPrecisionIdRangePerDimension();
				// if the id range is less than or equal to the target
				// resolution in each dimension, use this substrategy
				boolean withinTargetResolution = true;
				for (int d = 0; d < highestPrecisionIdRangePerDimension.length; d++) {
					if (highestPrecisionIdRangePerDimension[d] > targetResolutionPerDimension[d]) {
						withinTargetResolution = false;
						break;
					}
				}
				if (withinTargetResolution) {
					targetIndexStrategy = subStrategy;
					break;
				}
			}
			if (targetIndexStrategy == null) {
				// if there is not a substrategy that is within the target
				// resolution, use the first substrategy (the lowest range per
				// dimension, which is the highest precision)
				targetIndexStrategy = sortedStrategies.firstEntry().getValue();
			}
			return m_dataStore.query(
					adapter,
					new CustomIdIndex(
							// replace the index strategy with a single
							// substrategy that fits the target resolution
							targetIndexStrategy.getIndexStrategy(),
							index.getIndexModel(),
							index.getId()), // make sure the index ID is the
					// same as the orginal so that we
					// are querying the correct table
					query);
		}
		else {
			return m_dataStore.query(
					adapter,
					index,
					query);
		}
	}
}
