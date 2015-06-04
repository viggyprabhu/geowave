/**
 * 
 */
package mil.nga.giat.geowave.datastore.hbase;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;

/**
 * @author viggy
 * 
 */
public class HBaseAdapterStore implements
		AdapterStore
{

	public HBaseAdapterStore(
			BasicHBaseOperations instance ) {
		// TODO Auto-generated constructor stub
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * mil.nga.giat.geowave.core.store.adapter.AdapterStore#addAdapter(mil.nga
	 * .giat.geowave.core.store.adapter.DataAdapter)
	 */
	@Override
	public void addAdapter(
			DataAdapter<?> adapter ) {
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * mil.nga.giat.geowave.core.store.adapter.AdapterStore#getAdapter(mil.nga
	 * .giat.geowave.core.index.ByteArrayId)
	 */
	@Override
	public DataAdapter<?> getAdapter(
			ByteArrayId adapterId ) {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * mil.nga.giat.geowave.core.store.adapter.AdapterStore#adapterExists(mil
	 * .nga.giat.geowave.core.index.ByteArrayId)
	 */
	@Override
	public boolean adapterExists(
			ByteArrayId adapterId ) {
		// TODO Auto-generated method stub
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see mil.nga.giat.geowave.core.store.adapter.AdapterStore#getAdapters()
	 */
	@Override
	public CloseableIterator<DataAdapter<?>> getAdapters() {
		// TODO Auto-generated method stub
		return null;
	}

}
