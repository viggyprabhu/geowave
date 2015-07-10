package mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.hbase;

import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.ingest.IngestHBasePluginBase;

/**
 * This interface is used by the IngestFromHdfsPlugin to implement ingestion
 * within a mapper only. The implementation will be directly persisted to a
 * mapper and called to produce GeoWaveData to be written.
 * 
 * @param <I>
 *            data type for intermediate data
 * @param <O>
 *            data type that will be ingested into GeoWave
 */
public interface IngestWithHBaseMapper<I, O> extends
		IngestHBasePluginBase<I, O>,
		Persistable
{

}
