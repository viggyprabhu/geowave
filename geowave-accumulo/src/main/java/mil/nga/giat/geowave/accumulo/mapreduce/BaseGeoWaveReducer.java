package mil.nga.giat.geowave.accumulo.mapreduce;

import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputKey;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class BaseGeoWaveReducer<KEYOUT, VALUEOUT> extends
		Reducer<GeoWaveInputKey, Writable, KEYOUT, VALUEOUT>
{

}
