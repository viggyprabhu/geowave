/**
 * 
 */
package mil.nga.giat.geowave.datastore.hbase;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.datastore.hbase.metadata.AbstractHBasePersistence;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;

import org.apache.log4j.Logger;

/**
 * @author viggy
 * 
 */
public class HBaseDataStatisticsStore extends
		AbstractHBasePersistence<DataStatistics<?>> implements
		DataStatisticsStore
{

	private static final String STATISTICS_CF = "STATS";
	private final static Logger LOGGER = Logger.getLogger(HBaseDataStatisticsStore.class);

	public HBaseDataStatisticsStore(
			BasicHBaseOperations operations ) {
		super(
				operations);
	}

	@Override
	public void setStatistics(
			DataStatistics<?> statistics ) {
		removeStatistics(
				statistics.getDataAdapterId(),
				statistics.getStatisticsId());
		addObject(statistics);

	}

	@Override
	public void incorporateStatistics(
			DataStatistics<?> statistics ) {
		addObject(statistics);

	}

	@Override
	public CloseableIterator<DataStatistics<?>> getDataStatistics(
			ByteArrayId adapterId,
			String... authorizations ) {
		return getAllObjectsWithSecondaryId(
				adapterId,
				authorizations);
	}

	@Override
	public CloseableIterator<DataStatistics<?>> getAllDataStatistics(
			String... authorizations ) {
		return getObjects(authorizations);
	}

	@Override
	public DataStatistics<?> getDataStatistics(
			ByteArrayId adapterId,
			ByteArrayId statisticsId,
			String... authorizations ) {
		return getObject(
				statisticsId,
				adapterId,
				authorizations);
	}

	@Override
	public boolean removeStatistics(
			ByteArrayId adapterId,
			ByteArrayId statisticsId,
			String... authorizations ) {
		return deleteObject(
				statisticsId,
				adapterId,
				authorizations);
	}

	@Override
	protected ByteArrayId getPrimaryId(
			DataStatistics<?> persistedObject ) {
		return persistedObject.getStatisticsId();
	}

	@Override
	protected String getPersistenceTypeName() {
		return STATISTICS_CF;
	}

	@Override
	protected ByteArrayId getSecondaryId(
			final DataStatistics<?> persistedObject ) {
		return persistedObject.getDataAdapterId();
	}

}
