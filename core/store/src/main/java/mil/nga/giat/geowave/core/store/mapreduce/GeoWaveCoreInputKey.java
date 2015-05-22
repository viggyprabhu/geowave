/**
 * 
 */
package mil.nga.giat.geowave.core.store.mapreduce;

import mil.nga.giat.geowave.core.index.ByteArrayId;

/**
 * @author viggy
 * 
 */
public class GeoWaveCoreInputKey extends
		GeoWaveCoreKey
{

	private ByteArrayId dataId;

	public GeoWaveCoreInputKey() {
		super();
	}

	public GeoWaveCoreInputKey(
			final ByteArrayId adapterId,
			final ByteArrayId dataId ) {

		super(
				adapterId);
		this.dataId = dataId;
	}

	public ByteArrayId getDataId() {
		return dataId;
	}

}
