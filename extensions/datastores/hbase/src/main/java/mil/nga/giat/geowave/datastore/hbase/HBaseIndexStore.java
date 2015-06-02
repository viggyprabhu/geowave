/**
 * 
 */
package mil.nga.giat.geowave.datastore.hbase;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;

/**
 * @author viggy
 *
 */
public class HBaseIndexStore implements IndexStore {

	public HBaseIndexStore(BasicHBaseOperations instance) {
		// TODO Auto-generated constructor stub
	}

	/* (non-Javadoc)
	 * @see mil.nga.giat.geowave.core.store.index.IndexStore#addIndex(mil.nga.giat.geowave.core.store.index.Index)
	 */
	@Override
	public void addIndex(Index index) {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see mil.nga.giat.geowave.core.store.index.IndexStore#getIndex(mil.nga.giat.geowave.core.index.ByteArrayId)
	 */
	@Override
	public Index getIndex(ByteArrayId indexId) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see mil.nga.giat.geowave.core.store.index.IndexStore#indexExists(mil.nga.giat.geowave.core.index.ByteArrayId)
	 */
	@Override
	public boolean indexExists(ByteArrayId indexId) {
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see mil.nga.giat.geowave.core.store.index.IndexStore#getIndices()
	 */
	@Override
	public CloseableIterator<Index> getIndices() {
		// TODO Auto-generated method stub
		return null;
	}

}
