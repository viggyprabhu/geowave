/**
 * 
 */
package mil.nga.giat.geowave.adapter.raster;

import java.util.Iterator;
import java.util.List;

import mil.nga.giat.geowave.adapter.raster.adapter.MosaicPerPyramidLevelBuilder;
import mil.nga.giat.geowave.core.iface.combiner.ICombiner;
import mil.nga.giat.geowave.core.iface.combiner.IMergingVisibilityCombiner;
import mil.nga.giat.geowave.core.iface.field.IColumn;
import mil.nga.giat.geowave.core.iface.field.IColumnSet;
import mil.nga.giat.geowave.core.iface.store.IJobContextAdapterStore;
import mil.nga.giat.geowave.core.iface.store.IJobContextIndexStore;
import mil.nga.giat.geowave.core.iface.store.IStoreCombiner;
import mil.nga.giat.geowave.core.iface.store.StoreOperations;
import mil.nga.giat.geowave.core.iface.store.client.IIteratorScope;
import mil.nga.giat.geowave.core.iface.store.client.IIteratorSetting;
import mil.nga.giat.geowave.core.index.HierarchicalNumericIndexStrategy.SubStrategy;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.DataStoreFactory;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.StoreException;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.index.IndexStore;

import org.opengis.coverage.grid.GridCoverage;

/**
 * @author viggy
 * 
 */
public class RasterHelper
{

	public static IMergingVisibilityCombiner getMergingVisibilityCombiner() {
		// TODO #238 Need to give access to MergingVisibilityCombiner from here
		return null;
	}

	public static IColumnSet getNewColumnSet(
			List<String> asList ) {
		// TODO #238 Need to pass a new ColumnSet instance
		return null;
	}

	public static IJobContextIndexStore getJobContextIndexStore() {
		return DataStoreFactory.getFactory().getJobContextIndexStore();
	}

	public static IJobContextAdapterStore getJobContextAdapterStore() {
		// TODO #238 Need to pass a new JobContextAdapterStore instance
		return null;
	}

	public static DataStore getDataStore(
			StoreOperations ops ) {
		// TODO #238 Need to pass a new AccumuloDataStore
		return null;
	}

	public static IndexStore getIndexStore(
			StoreOperations ops ) {
		// TODO #238 Need to pass a new AccumuloIndexStore
		return null;
	}

	public static StoreOperations getStoreOperations(
			String zookeeper,
			String instance,
			String user,
			String password,
			String newNamespace )
			throws StoreException {
		// TODO #238 Need to pass a new BasicAccumuloOperations
		return DataStoreFactory.getFactory().getStoreOperations(
				zookeeper,
				instance,
				user,
				password,
				newNamespace);
	}

	public static AdapterStore getAdapterStore(
			StoreOperations oldNamespaceOperations ) {
		// TODO Need to pass a new AccumuloAdapterStore
		return null;
	}

	public static DataStatisticsStore getDataStatisticsStore(
			StoreOperations accumuloOperations ) {
		// TODO Auto-generated method stub
		return null;
	}

	public static Iterator<GridCoverage> getIteratorWrapper(
			Iterator<SubStrategy> iterator,
			MosaicPerPyramidLevelBuilder mosaicPerPyramidLevelBuilder ) {
		// TODO Auto-generated method stub
		return null;
	}

	public static IIteratorScope getIteratorScope() {
		// TODO Auto-generated method stub
		return null;
	}

	public static IIteratorSetting getIteratorSetting(
			int rasterTileCombinerPriority,
			Class<? extends ICombiner> class1 ) {
		// TODO Auto-generated method stub
		return null;
	}

	public static IColumn getColumn(
			String coverageName ) {
		// TODO Auto-generated method stub
		return null;
	}

	public static IStoreCombiner getCombiner() {
		// TODO Auto-generated method stub
		return null;
	}

}
