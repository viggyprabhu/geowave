package mil.nga.giat.geowave.test;

import java.io.File;

import mil.nga.giat.geowave.accumulo.mapreduce.dedupe.GeoWaveDedupeJobRunner;
import mil.nga.giat.geowave.ingest.IngestMain;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.store.query.DistributableQuery;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

public class GeoWaveMapReduceIT extends
		GeoWaveTestEnvironment
{
	private final static Logger LOGGER = Logger.getLogger(GeoWaveMapReduceIT.class);
	private static final String TEST_RESOURCE_PACKAGE = "mil/nga/giat/geowave/test/";
	private static final String TEST_DATA_ZIP_RESOURCE_PATH = TEST_RESOURCE_PACKAGE + "mapreduce-testdata.zip";
	private static final String TEST_CASE_GENERAL_GPX_BASE = TEST_CASE_BASE + "general_gpx_test_case/";
	private static final String GENERAL_GPX_FILTER_PACKAGE = TEST_CASE_GENERAL_GPX_BASE + "filter/";
	private static final String GENERAL_GPX_FILTER_FILE = GENERAL_GPX_FILTER_PACKAGE + "filter.shp";
	private static final String GENERAL_GPX_INPUT_GPX_DIR = TEST_CASE_GENERAL_GPX_BASE + "input_gpx/";
	private static final String GENERAL_GPX_INPUT_SHP_DIR = TEST_CASE_GENERAL_GPX_BASE + "input_shp/";
	private static final String GENERAL_GPX_EXPECTED_RESULTS_DIR = TEST_CASE_GENERAL_GPX_BASE + "filter_results/";
	private static final String OSM_GPX_INPUT_DIR = TEST_CASE_BASE + "osm_gpx_test_case/";

	@BeforeClass
	public static void extractTestFiles() {
		GeoWaveTestEnvironment.unZipFile(
				GeoWaveMapReduceIT.class.getClassLoader().getResourceAsStream(
						TEST_DATA_ZIP_RESOURCE_PATH),
				TEST_CASE_BASE);
	}

	private void testIngest(
			final IndexType indexType,
			final String ingestFilePath ) {
		// ingest gpx data directly into GeoWave using the
		// ingest framework's main method and pre-defined commandline arguments
		LOGGER.warn("Ingesting '" + ingestFilePath + "' - this may take several minutes...");
		final String[] args = StringUtils.split(
				"-hdfsingest -t gpx -hdfs file:/// -hdfsbase " + tempDir + File.separator + "tmp -jobtracker local -b " + ingestFilePath + " -z " + zookeeper + " -i " + accumuloInstance + " -u " + accumuloUser + " -p " + accumuloPassword + " -n " + TEST_NAMESPACE + " -dim " + (indexType.equals(IndexType.SPATIAL_VECTOR) ? "spatial" : "spatial-temporal"),
				' ');
		IngestMain.main(args);
	}

	@Test
	public void testIngestAndQueryGeneralGpx() {
		testIngest(
				IndexType.SPATIAL_VECTOR,
				GENERAL_GPX_INPUT_GPX_DIR);
	}

	@Test
	public void testIngestAndQueryOsmGpx() {
		testIngest(
				IndexType.SPATIAL_VECTOR,
				OSM_GPX_INPUT_DIR);
		testIngest(
				IndexType.SPATIAL_TEMPORAL_VECTOR,
				OSM_GPX_INPUT_DIR);
	}

	private static class TestJobRunner extends
			GeoWaveDedupeJobRunner
	{
		@Override
		protected DistributableQuery getQuery() {
			return super.getQuery();
		}
	}
}
