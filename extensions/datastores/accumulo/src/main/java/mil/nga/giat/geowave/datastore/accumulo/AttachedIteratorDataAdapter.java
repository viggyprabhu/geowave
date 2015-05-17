package mil.nga.giat.geowave.datastore.accumulo;

import mil.nga.giat.geowave.core.iface.store.CoreAttachedIteratorDataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.mapreduce.client.CoreIteratorConfig;

public interface AttachedIteratorDataAdapter<T> extends
		CoreAttachedIteratorDataAdapter<T>
{
	public static final String ATTACHED_ITERATOR_CACHE_ID = "AttachedIterators";

	public CoreIteratorConfig[] getAttachedIteratorConfig(
			final Index index );
}
