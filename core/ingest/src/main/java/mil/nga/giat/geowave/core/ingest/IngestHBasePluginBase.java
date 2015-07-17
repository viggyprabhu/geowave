/**
 * 
 */
package mil.nga.giat.geowave.core.ingest;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;

/**
 * @author viggy
 * 
 */
public interface IngestHBasePluginBase<I, O> extends
		DataAdapterProvider<O>
{

	public CloseableIterator<GeoWaveHBaseData<O>> toGeoWaveData(
			I input,
			ByteArrayId primaryIndexId,
			String globalVisibility );
}
