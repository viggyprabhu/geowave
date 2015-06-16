package mil.nga.giat.geowave.datastore.hbase.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

import mil.nga.giat.geowave.core.store.ScanCallback;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.Index;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

/**
 * This is used internally to translate Accumulo rows into native objects (using
 * the appropriate data adapter). It also performs any client-side filtering. It
 * will peek at the next entry in the accumulo iterator to always maintain a
 * reference to the next value.
 * 
 * @param <T>
 *            The type for the entry
 */
public class EntryIteratorWrapper<T> implements
		Iterator<T>
{
	private final static Logger LOGGER = Logger.getLogger(EntryIteratorWrapper.class);
	private final AdapterStore adapterStore;
	private final Index index;
	private final Scan scannerIt;
	private final QueryFilter clientFilter;
	private final ScanCallback<T> scanCallback;

	private T nextValue;

	public EntryIteratorWrapper(
			final AdapterStore adapterStore,
			final Index index,
			final Scan scannerIt,
			final QueryFilter clientFilter ) {
		this.adapterStore = adapterStore;
		this.index = index;
		this.scannerIt = scannerIt;
		this.clientFilter = clientFilter;
		this.scanCallback = null;
	}

	public EntryIteratorWrapper(
			final AdapterStore adapterStore,
			final Index index,
			final Scan scannerIt,
			final QueryFilter clientFilter,
			final ScanCallback<T> scanCallback ) {
		this.adapterStore = adapterStore;
		this.index = index;
		this.scannerIt = scannerIt;
		this.clientFilter = clientFilter;
		this.scanCallback = scanCallback;
	}

	@Override
	public T next()
			throws NoSuchElementException {
		final T previousNext = nextValue;
		if (nextValue == null) {
			throw new NoSuchElementException();
		}
		nextValue = null;
		return previousNext;
	}

	@Override
	public boolean hasNext() {
		// TODO #406 Need to fix
		LOGGER.error("This method is not yet coded. Need to fix it");
		return false;
	}

	@Override
	public void remove() {
		// TODO #406 Need to fix
		LOGGER.error("This method is not yet coded. Need to fix it");
	}

}
