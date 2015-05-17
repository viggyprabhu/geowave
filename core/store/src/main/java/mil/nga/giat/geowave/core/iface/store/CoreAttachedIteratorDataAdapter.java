/**
 * 
 */
package mil.nga.giat.geowave.core.iface.store;

import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.mapreduce.client.CoreIteratorConfig;

/**
 * @author viggy
 *
 */
public interface CoreAttachedIteratorDataAdapter<T> extends
		WritableDataAdapter<T> {

	public static final String ATTACHED_ITERATOR_CACHE_ID = "AttachedIterators";

	public CoreIteratorConfig[] getAttachedIteratorConfig(
			final Index index );
}
