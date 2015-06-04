/**
 * 
 */
package mil.nga.giat.geowave.datastore.hbase;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;

/**
 * @author viggy
 * 
 */
public class HBaseDataStatisticsStore implements
		DataStatisticsStore
{

	public HBaseDataStatisticsStore(
			BasicHBaseOperations instance ) {
		// TODO Auto-generated constructor stub
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore
	 * #setStatistics
	 * (mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics)
	 */
	@Override
	public void setStatistics(
			DataStatistics<?> statistics ) {
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore
	 * #incorporateStatistics
	 * (mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics)
	 */
	@Override
	public void incorporateStatistics(
			DataStatistics<?> statistics ) {
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore
	 * #getDataStatistics(mil.nga.giat.geowave.core.index.ByteArrayId,
	 * java.lang.String[])
	 */
	@Override
	public CloseableIterator<DataStatistics<?>> getDataStatistics(
			ByteArrayId adapterId,
			String... authorizations ) {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore
	 * #getAllDataStatistics(java.lang.String[])
	 */
	@Override
	public CloseableIterator<DataStatistics<?>> getAllDataStatistics(
			String... authorizations ) {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore
	 * #getDataStatistics(mil.nga.giat.geowave.core.index.ByteArrayId,
	 * mil.nga.giat.geowave.core.index.ByteArrayId, java.lang.String[])
	 */
	@Override
	public DataStatistics<?> getDataStatistics(
			ByteArrayId adapterId,
			ByteArrayId statisticsId,
			String... authorizations ) {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore
	 * #removeStatistics(mil.nga.giat.geowave.core.index.ByteArrayId,
	 * mil.nga.giat.geowave.core.index.ByteArrayId, java.lang.String[])
	 */
	@Override
	public boolean removeStatistics(
			ByteArrayId adapterId,
			ByteArrayId statisticsId,
			String... authorizations ) {
		// TODO Auto-generated method stub
		return false;
	}

}
