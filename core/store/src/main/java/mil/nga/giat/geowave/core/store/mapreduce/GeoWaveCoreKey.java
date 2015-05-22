/**
 * 
 */
package mil.nga.giat.geowave.core.store.mapreduce;

import mil.nga.giat.geowave.core.index.ByteArrayId;

/**
 * @author viggy
 * 
 */
public abstract class GeoWaveCoreKey
{

	protected ByteArrayId adapterId;

	protected GeoWaveCoreKey() {}

	public GeoWaveCoreKey(
			ByteArrayId adapterId ) {
		this.adapterId = adapterId;
	}

	public ByteArrayId getAdapterId() {
		return adapterId;
	}
}
