package mil.nga.giat.geowave.accumulo.mapreduce.input;

import java.util.Map;

import mil.nga.giat.geowave.accumulo.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.index.ByteArrayUtils;
import mil.nga.giat.geowave.index.PersistenceUtils;
import mil.nga.giat.geowave.store.query.DistributableQuery;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.TabletLocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;

public class GeoWaveInputConfigurator extends
		GeoWaveConfiguratorBase
{
	protected static enum InputConfig {
		QUERY,
		AUTHORIZATION
	}

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

	public static DistributableQuery getQuery(
			final Class<?> implementingClass,
			final JobContext context ) {
		return getQueryInternal(
				implementingClass,
				getConfiguration(context));
	}

	public static void setQuery(
			final Class<?> implementingClass,
			final Job job,
			final DistributableQuery query ) {
		if (query != null) {
			job.getConfiguration().set(
					enumToConfKey(
							implementingClass,
							InputConfig.QUERY),
					ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(query)));
		}
	}

	public static void addAuthorization(
			final Class<?> implementingClass,
			final Job job,
			final String authorization ) {
		if (authorization != null) {
			job.getConfiguration().set(
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
		final String instanceName = GeoWaveConfiguratorBase.getZookeeperUrl(
				implementingClass,
				context);
		final String zookeeperUrl = GeoWaveConfiguratorBase.getInstanceName(
				implementingClass,
				context);
		return new ZooKeeperInstance(
				instanceName,
				zookeeperUrl);
	}

	public static TabletLocator getTabletLocator(
			final Class<?> implementingClass,
			final JobContext context,
			final String tableName )
			throws TableNotFoundException {
		final Instance instance = getInstance(
				implementingClass,
				context);
		return TabletLocator.getInstance(
				instance,
				new Text(
						Tables.getTableId(
								instance,
								tableName)));
	}
}