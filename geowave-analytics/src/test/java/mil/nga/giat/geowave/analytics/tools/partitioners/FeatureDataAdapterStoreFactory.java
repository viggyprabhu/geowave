package mil.nga.giat.geowave.analytics.tools.partitioners;

import mil.nga.giat.geowave.analytics.parameters.ParameterEnum;
import mil.nga.giat.geowave.analytics.tools.ConfigurationWrapper;
import mil.nga.giat.geowave.analytics.tools.PropertyManagement;
import mil.nga.giat.geowave.analytics.tools.RunnerUtils;
import mil.nga.giat.geowave.analytics.tools.dbops.AdapterStoreFactory;
import mil.nga.giat.geowave.index.PersistenceUtils;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.adapter.MemoryAdapterStore;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeatureDataAdapterStoreFactory implements
		AdapterStoreFactory
{
	final static Logger LOGGER = LoggerFactory.getLogger(FeatureDataAdapterStoreFactory.class);
	FeatureDataAdapter adapter = null;

	enum MyData
			implements
			ParameterEnum {

		DATA(
				byte[].class);

		private final Class<?> baseClass;

		MyData(
				final Class<?> baseClass ) {
			this.baseClass = baseClass;
		}

		@Override
		public Class<?> getBaseClass() {
			return baseClass;
		}

		@Override
		public Enum<?> self() {
			return this;
		}

	}

	public static void transferState(
			Configuration configuration,
			PropertyManagement runTimeProperties ) {
		RunnerUtils.setParameter(
				configuration,
				FeatureDataAdapterStoreFactory.class,
				runTimeProperties,
				new ParameterEnum[] {
					MyData.DATA
				});
	}

	public static void saveState(
			FeatureDataAdapter dataAdapter,
			PropertyManagement runTimeProperties ) {
		runTimeProperties.store(
				MyData.DATA,
				PersistenceUtils.toBinary(dataAdapter));
	}

	@Override
	public synchronized AdapterStore getAdapterStore(
			ConfigurationWrapper context )
			throws InstantiationException {
		return new MemoryAdapterStore(
				new DataAdapter[] {
					PersistenceUtils.fromBinary(
							context.getBytes(
									MyData.DATA,
									this.getClass()),
							DataAdapter.class)
				});
	}

}
