package mil.nga.giat.geowave.accumulo.util;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.filter.QueryFilter;
import mil.nga.giat.geowave.store.index.Index;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.collections.Transformer;

public class EntryWithKeyIteratorWrapper<T> implements
		Iterator<Entry<Key, T>>
{
	private EntryIteratorWrapper<T> entryIteratorWrapper;
	private Key currentKey;

	public EntryWithKeyIteratorWrapper(
			final AdapterStore adapterStore,
			final Index index,
			final Iterator<Entry<Key, Value>> scannerIt,
			final QueryFilter clientFilter ) {
		entryIteratorWrapper = new EntryIteratorWrapper(
				adapterStore,
				index,
				IteratorUtils.transformedIterator(
						scannerIt,
						new Transformer() {

							@Override
							public Object transform(
									final Object obj ) {
								final Entry<Key, Value> entry = (Entry<Key, Value>) obj;
								if (entry != null) {
									currentKey = entry.getKey();
								}
								return obj;
							}
						}),
				clientFilter);
	}

	@Override
	public boolean hasNext() {
		return entryIteratorWrapper.hasNext();
	}

	@Override
	public Entry<Key, T> next() {
		final T value = entryIteratorWrapper.next();
		return new GeoWaveEntry(
				currentKey,
				value);
	}

	@Override
	public void remove() {
		entryIteratorWrapper.remove();
	}

	private final class GeoWaveEntry implements
			Map.Entry<Key, T>
	{
		private final Key key;
		private T value;

		public GeoWaveEntry(
				final Key key,
				final T value ) {
			this.key = key;
			this.value = value;
		}

		@Override
		public Key getKey() {
			return key;
		}

		@Override
		public T getValue() {
			return value;
		}

		@Override
		public T setValue(
				final T value ) {
			final T old = this.value;
			this.value = value;
			return old;
		}
	}
}
