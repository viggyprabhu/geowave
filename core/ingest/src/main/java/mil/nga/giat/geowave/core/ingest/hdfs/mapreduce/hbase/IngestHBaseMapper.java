package mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.hbase;

import java.io.IOException;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.ingest.GeoWaveHBaseData;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.datastore.hbase.mapreduce.output.GeoWaveHBaseOutputKey;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * This class is the map-reduce mapper for ingestion with the mapper only.
 */
public class IngestHBaseMapper extends
		Mapper<AvroKey, NullWritable, GeoWaveHBaseOutputKey, Object>
{
	private IngestWithHBaseMapper ingestWithMapper;
	private String globalVisibility;
	private ByteArrayId primaryIndexId;

	@Override
	protected void map(
			final AvroKey key,
			final NullWritable value,
			final org.apache.hadoop.mapreduce.Mapper.Context context )
			throws IOException,
			InterruptedException {
		try (CloseableIterator<GeoWaveHBaseData> data = ingestWithMapper.toGeoWaveData(
				key.datum(),
				primaryIndexId,
				globalVisibility)) {
			while (data.hasNext()) {
				final GeoWaveHBaseData d = data.next();
				context.write(
						d.getKey(),
						d.getValue());
			}
		}
	}

	@Override
	protected void setup(
			final org.apache.hadoop.mapreduce.Mapper.Context context )
			throws IOException,
			InterruptedException {
		super.setup(context);
		try {
			final String ingestWithMapperStr = context.getConfiguration().get(
					AbstractMapReduceHBaseIngest.INGEST_PLUGIN_KEY);
			final byte[] ingestWithMapperBytes = ByteArrayUtils.byteArrayFromString(ingestWithMapperStr);
			ingestWithMapper = PersistenceUtils.fromBinary(
					ingestWithMapperBytes,
					IngestWithHBaseMapper.class);
			globalVisibility = context.getConfiguration().get(
					AbstractMapReduceHBaseIngest.GLOBAL_VISIBILITY_KEY);
			final String primaryIndexIdStr = context.getConfiguration().get(
					AbstractMapReduceHBaseIngest.PRIMARY_INDEX_ID_KEY);
			if (primaryIndexIdStr != null) {
				primaryIndexId = new ByteArrayId(
						primaryIndexIdStr);
			}
		}
		catch (final Exception e) {
			throw new IllegalArgumentException(
					e);
		}
	}
}
