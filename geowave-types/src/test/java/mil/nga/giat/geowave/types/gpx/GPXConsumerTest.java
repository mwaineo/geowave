package mil.nga.giat.geowave.types.gpx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.ingest.GeoWaveData;
import mil.nga.giat.geowave.types.HelperClass;
import mil.nga.giat.geowave.types.ValidateObject;

import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;

public class GPXConsumerTest
{

	Map<String, ValidateObject<SimpleFeature>> expectedResults = new HashMap<String, ValidateObject<SimpleFeature>>();

	@Before
	public void setup() {

		expectedResults.put(
				"123_6_A_track_1_1",
				new ValidateObject<SimpleFeature>() {
					@Override
					public boolean validate(
							SimpleFeature feature ) {
						return feature.getAttribute(
								"Elevation").toString().equals(
								"4.46") && feature.getAttribute("Timestamp") != null && feature.getAttribute("Latitude") != null && feature.getAttribute("Longitude") != null;
					}
				});
		expectedResults.put(
				"123_6_A_track_1_2",
				new ValidateObject<SimpleFeature>() {
					@Override
					public boolean validate(
							SimpleFeature feature ) {
						return feature.getAttribute(
								"Elevation").toString().equals(
								"4.634") && feature.getAttribute("Timestamp") != null && feature.getAttribute("Latitude") != null && feature.getAttribute("Longitude") != null;
					}
				});
		expectedResults.put(
				"123_2_B_track_1_1",
				new ValidateObject<SimpleFeature>() {
					@Override
					public boolean validate(
							SimpleFeature feature ) {
						return feature.getAttribute(
								"Elevation").toString().equals(
								"10.46") && feature.getAttribute("Timestamp") != null && feature.getAttribute("Latitude") != null && feature.getAttribute("Longitude") != null;
					}
				});
		expectedResults.put(
				"123_2_B_track_1_2",
				new ValidateObject<SimpleFeature>() {
					@Override
					public boolean validate(
							SimpleFeature feature ) {
						return feature.getAttribute(
								"Elevation").toString().equals(
								"11.634") && feature.getAttribute(
								"Fix").toString().equals(
								"2d") && feature.getAttribute(
								"Satellites").toString().equals(
								"8") && feature.getAttribute(
								"HDOP").toString().equals(
								"2.0") && feature.getAttribute(
								"VDOP").toString().equals(
								"2.1") && feature.getAttribute(
								"PDOP").toString().equals(
								"2.2") && feature.getAttribute("Timestamp") != null && feature.getAttribute("Latitude") != null && feature.getAttribute("Longitude") != null;
					}
				});
		expectedResults.put(
				"123_6_A_track",
				new ValidateObject<SimpleFeature>() {
					@Override
					public boolean validate(
							SimpleFeature feature ) {
						return feature.getAttribute(
								"Name").toString().equals(
								"A track") && feature.getAttribute(
								"Duration").toString().equals(
								"60000") && feature.getAttribute("StartTimeStamp") != null && feature.getAttribute(
								"NumberPoints").toString().equals(
								"2") && feature.getAttribute("EndTimeStamp") != null;
					}
				});
		expectedResults.put(
				"123_2_B_track",
				new ValidateObject<SimpleFeature>() {
					@Override
					public boolean validate(
							SimpleFeature feature ) {
						return feature.getAttribute(
								"Duration").toString().equals(
								"60000") && feature.getAttribute("StartTimeStamp") != null && feature.getAttribute(
								"NumberPoints").toString().equals(
								"2") && feature.getAttribute("EndTimeStamp") != null;
					}
				});
		expectedResults.put(
				"AQUADUCT_0422469500_-0714618070",
				new ValidateObject<SimpleFeature>() {
					@Override
					public boolean validate(
							SimpleFeature feature ) {
						return feature.getAttribute(
								"Description").toString().equals(
								"Aquaduct") && feature.getAttribute("Longitude") != null && feature.getAttribute(
								"Symbol").toString().equals(
								"Dam") && feature.getAttribute("Latitude") != null;
					}
				});
		expectedResults.put(
				"TRANSITION_0422446460_-0714685390",
				new ValidateObject<SimpleFeature>() {
					@Override
					public boolean validate(
							SimpleFeature feature ) {
						return feature.getAttribute(
								"Name").toString().equals(
								"TRANSITION") && feature.getAttribute(
								"Elevation").toString().equals(
								"92.6592");
					}
				});
		expectedResults.put(
				"123_12_ROUT135ASP",
				new ValidateObject<SimpleFeature>() {
					@Override
					public boolean validate(
							SimpleFeature feature ) {
						return feature.getAttribute(
								"Name").toString().equals(
								"ROUT135ASP") && feature.getAttribute(
								"NumberPoints").toString().equals(
								"2") && feature.getAttribute(
								"Description").toString().equals(
								"Route 135 ASP");
					}
				});

		expectedResults.put(
				"123_12_ROUT135ASP_2_rtename2_0422446460_-0714685390",
				new ValidateObject<SimpleFeature>() {
					@Override
					public boolean validate(
							SimpleFeature feature ) {
						return feature.getAttribute("Longitude") != null && feature.getAttribute("Latitude") != null;
					}
				});

	}

	@Test
	public void test()
			throws IOException {
		Set<String> expectedSet = HelperClass.buildSet(expectedResults);

		InputStream is = this.getClass().getClassLoader().getResourceAsStream(
				"sample_gpx.xml");
		GPXConsumer consumer = new GPXConsumer(
				is,
				new ByteArrayId(
						"123".getBytes()),
				"123",
				new HashMap<String, Map<String, String>>(),
				true,
				"");
		int totalCount = 0;

		while (consumer.hasNext()) {
			GeoWaveData<SimpleFeature> data = consumer.next();
			expectedSet.remove(data.getValue().getID());
			ValidateObject<SimpleFeature> tester = expectedResults.get(data.getValue().getID());
			if (tester != null) {
				assertTrue(
						data.getValue().toString(),
						tester.validate(data.getValue()));
			}
			totalCount++;
		}
		consumer.close();
		assertEquals(
				12,
				totalCount);
		// did everything get validated?
		if (expectedSet.size() > 0) {
			System.out.println("Failed matches:");
			System.out.println(expectedSet);
		}
		assertEquals(
				"All expected data set should be matched; zero unmatched data expected",
				0,
				expectedSet.size());
	}

	/**
	 * run test and each duplicate is treated uniquely
	 * 
	 * @throws IOException
	 */
	@Test
	public void testDescent()
			throws IOException {
		descent(new File(
				"src/test/resources/gpx"));
	}

	private static Map<String, Long> fileCount = new HashMap<String, Long>();

	static {
		fileCount.put(
				"000991807.gpx",
				new Long(
						40));
		/** tests duplicate waypoint **/
		fileCount.put(
				"mystic_basin_trail.gpx",
				new Long(
						24));
	}

	private void descent(
			File dir )
			throws IOException {
		if (dir.isDirectory()) {
			for (File file : dir.listFiles())
				descent(file);

		}
		else if (dir.getName().endsWith(
				"gpx")) {
			InputStream is = new FileInputStream(
					dir);
			try {
				GPXConsumer consumer = new GPXConsumer(
						is,
						new ByteArrayId(
								"123".getBytes()),
						"",
						new HashMap<String, Map<String, String>>(),
						false,
						"");
				Set<String> ids = new HashSet<String>();
				while (consumer.hasNext()) {
					String id = consumer.next().getValue().getID();
					// insure uniqueness...even for duplicate points
					assertTrue(!ids.contains(id));
					ids.add(id);
				}
				Long amount = fileCount.get(dir.getName());
				if (amount != null) {
					assertEquals(
							dir.getName(),
							amount.intValue(),
							ids.size());
				}
			}
			catch (Exception ex) {
				System.out.println("Failed " + dir);
				throw ex;
			}
			finally {
				is.close();
			}

		}

	}
}
