/**
 * 
 */
package mil.nga.giat.geowave.core.store.mapreduce.hadoop;

import mil.nga.giat.geowave.core.store.mapreduce.GeoWaveCoreConfiguratorBase;

import org.apache.hadoop.conf.Configuration;

/**
 * @author viggy
 * 
 */
public class GeoWaveCoreInputConfigurator extends
		GeoWaveCoreConfiguratorBase
{

	public static enum InputConfig {
		QUERY,
		AUTHORIZATION,
		MIN_SPLITS,
		MAX_SPLITS,
		OUTPUT_WRITABLE // used to inform the input format to output a Writable
						// from the HadoopDataAdapter
	}

	public static void setMinimumSplitCount(
			final Class<?> implementingClass,
			final Configuration config,
			final Integer minSplits ) {
		if (minSplits != null) {
			config.set(
					enumToConfKey(
							implementingClass,
							InputConfig.MIN_SPLITS),
					minSplits.toString());
		}
		else {
			config.unset(enumToConfKey(
					implementingClass,
					InputConfig.MIN_SPLITS));
		}
	}

	public static void setMaximumSplitCount(
			final Class<?> implementingClass,
			final Configuration config,
			final Integer maxSplits ) {
		if (maxSplits != null) {
			config.set(
					enumToConfKey(
							implementingClass,
							InputConfig.MAX_SPLITS),
					maxSplits.toString());
		}
		else {
			config.unset(enumToConfKey(
					implementingClass,
					InputConfig.MAX_SPLITS));
		}
	}
}
