/**
 * 
 */
package mil.nga.giat.geowave.core.store.mapreduce;

import mil.nga.giat.geowave.core.index.ByteArrayId;

/**
 * @author viggy
 *
 */
public class GeoWaveCoreOutputKey extends GeoWaveCoreKey {

	private ByteArrayId indexId;

	protected GeoWaveCoreOutputKey() {
		super();
	}

	public GeoWaveCoreOutputKey(
			final ByteArrayId adapterId,
			final ByteArrayId indexId ) {
		super(adapterId);
		this.indexId = indexId;
	}

	public ByteArrayId getIndexId() {
		return indexId;
	}
	
}
