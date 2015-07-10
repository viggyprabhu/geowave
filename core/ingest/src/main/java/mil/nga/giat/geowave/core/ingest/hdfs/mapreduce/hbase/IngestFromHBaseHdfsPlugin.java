/**
 * 
 */
package mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.hbase;

import mil.nga.giat.geowave.core.ingest.avro.AvroPluginBase;
import mil.nga.giat.geowave.core.store.index.Index;

/**
 * @author viggy
 *
 */
public interface IngestFromHBaseHdfsPlugin<I, O> extends
AvroPluginBase
{
/**
* Returns a flag indicating to the ingestion framework whether it should
* try to use the ingestWithMapper() implementation or the
* ingestWithReducer() implementation in the case that both implementations
* are non-null.
* 
* @return If true, the framework will use ingestWithReducer() and only fall
*         back to ingestWithMapper() if necessary, otherwise the behavior
*         will be the reverse
*/
public boolean isUseReducerPreferred();

/**
* An implementation of ingestion that can be persisted to a mapper within
* the map-reduce job configuration to perform an ingest of data into
* GeoWave from intermediate data
* 
* @return The implementation for ingestion with only a mapper
*/
public IngestWithHBaseMapper<I, O> ingestWithMapper();

/**
* An implementation of ingestion that can be persisted to a mapper and
* reducer within the map-reduce job configuration to aggregate intermediate
* data by defined keys within a reducer and perform an ingest of data into
* GeoWave from the key-value pairs emitted by the mapper.
* 
* @return The implementation for ingestion with a mapper and reducer. It is
*         important to provide the correct concrete implementation of Key
*         and Value classes within the appropriate generics because the
*         framework will use reflection to set the key and value classes
*         for map-reduce.
*/
public IngestWithHBaseReducer<I, ?, ?, O> ingestWithReducer();

/**
* Get an array of indices that are supported by this ingestion
* implementation. This should be the full set of possible indices to use
* for this ingest type (for example both spatial and spatial-temporal, or
* perhaps just one).
* 
* @return the array of indices that are supported by this ingestion
*         implementation
*/
public Index[] getSupportedIndices();

/**
* Get an array of indices that are required by this ingestion
* implementation. This should be a subset of supported indices. All of
* these indices will automatically be persisted with GeoWave's metadata
* store and in the job configuration, whereas indices that are just
* "supported" will not automatically be persisted (only if they are the
* primary index). This is primarily useful if there is a supplemental index
* required by the ingestion process that is not the primary index.
* 
* @return the array of indices that are supported by this ingestion
*         implementation
*/
public Index[] getRequiredIndices();

}
