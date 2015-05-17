package mil.nga.giat.geowave.datastore.accumulo.mapreduce.input;

import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.DataStoreFactory;
import mil.nga.giat.geowave.core.store.adapter.StoreException;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.mapreduce.hadoop.GeoWaveCoreInputConfigurator;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloStoreUtils;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.GeoWaveConfiguratorBase;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * This class provides utility methods for accessing job context configuration
 * parameters that are specific to the GeoWaveInputFormat.
 */
public class GeoWaveInputConfigurator extends
		GeoWaveCoreInputConfigurator
{
	

	private static DistributableQuery getQueryInternal(
			final Class<?> implementingClass,
			final Configuration configuration ) {
		final String queryStr = configuration.get(
				enumToConfKey(
						implementingClass,
						InputConfig.QUERY),
				"");
		if ((queryStr != null) && !queryStr.isEmpty()) {
			final byte[] queryBytes = ByteArrayUtils.byteArrayFromString(queryStr);
			return PersistenceUtils.fromBinary(
					queryBytes,
					DistributableQuery.class);
		}
		return null;
	}

	private static Integer getMinimumSplitCountInternal(
			final Class<?> implementingClass,
			final Configuration configuration ) {
		return getIntegerConfigInternal(
				implementingClass,
				configuration,
				InputConfig.MIN_SPLITS);
	}

	private static Integer getMaximumSplitCountInternal(
			final Class<?> implementingClass,
			final Configuration configuration ) {
		return getIntegerConfigInternal(
				implementingClass,
				configuration,
				InputConfig.MAX_SPLITS);
	}

	private static Integer getIntegerConfigInternal(
			final Class<?> implementingClass,
			final Configuration configuration,
			final InputConfig inputConfig ) {
		final String str = configuration.get(
				enumToConfKey(
						implementingClass,
						inputConfig),
				"");
		if ((str != null) && !str.isEmpty()) {
			final Integer retVal = Integer.parseInt(str);
			return retVal;
		}
		return null;
	}

	public static DistributableQuery getQuery(
			final Class<?> implementingClass,
			final JobContext context ) {
		return getQueryInternal(
				implementingClass,
				getConfiguration(context));
	}

	public static void setQuery(
			final Class<?> implementingClass,
			final Configuration config,
			final DistributableQuery query ) {
		if (query != null) {
			config.set(
					enumToConfKey(
							implementingClass,
							InputConfig.QUERY),
					ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(query)));
		}
		else {
			config.unset(enumToConfKey(
					implementingClass,
					InputConfig.QUERY));
		}
	}

	public static Integer getMinimumSplitCount(
			final Class<?> implementingClass,
			final JobContext context ) {
		return getMinimumSplitCountInternal(
				implementingClass,
				getConfiguration(context));
	}

	public static Integer getMaximumSplitCount(
			final Class<?> implementingClass,
			final JobContext context ) {
		return getMaximumSplitCountInternal(
				implementingClass,
				getConfiguration(context));
	}

	public static void addAuthorization(
			final Class<?> implementingClass,
			final Configuration config,
			final String authorization ) {
		if (authorization != null) {
			config.set(
					enumToConfKey(
							implementingClass,
							InputConfig.AUTHORIZATION,
							authorization),
					authorization);
		}
	}

	public static String[] getAuthorizations(
			final Class<?> implementingClass,
			final JobContext context ) {
		return getAuthorizationsInternal(
				implementingClass,
				getConfiguration(context));
	}

	private static String[] getAuthorizationsInternal(
			final Class<?> implementingClass,
			final Configuration configuration ) {
		final Map<String, String> input = configuration.getValByRegex(enumToConfKey(
				implementingClass,
				InputConfig.AUTHORIZATION) + "*");
		if (input != null) {
			return input.values().toArray(
					new String[] {});
		}
		return new String[] {};
	}

	public static Instance getInstance(
			final Class<?> implementingClass,
			final JobContext context ) {
		final String instanceName = GeoWaveConfiguratorBase.getInstanceName(
				implementingClass,
				context);
		final String zookeeperUrl = GeoWaveConfiguratorBase.getZookeeperUrl(
				implementingClass,
				context);
		return new ZooKeeperInstance(
				instanceName,
				zookeeperUrl);
	}

	public static Index[] searchForIndices(
			final Class<?> implementingClass,
			final JobContext context ) {
		final Index[] userIndices = AccumuloStoreUtils.getJobContextIndexStore().getIndices(context);
		if ((userIndices == null) || (userIndices.length <= 0)) {
			try {
				// if there are no indices, assume we are searching all indices
				// in the metadata store
				return (Index[]) IteratorUtils.toArray( 
						DataStoreFactory.getFactory().getIndexStore(
															getAccumuloOperations(
																	implementingClass,
																	context)).getIndices(),
																	Index.class);
			}
			catch (StoreException e) {
				LOGGER.warn(
						"Unable to lookup indices from GeoWave metadata store",
						e);
			}
		}
		return userIndices;
	}
}