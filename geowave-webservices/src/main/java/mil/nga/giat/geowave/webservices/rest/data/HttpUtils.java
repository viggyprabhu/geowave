package mil.nga.giat.geowave.webservices.rest.data;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.log4j.Logger;

public class HttpUtils
{
	private static final Logger LOGGER = Logger.getLogger(HttpUtils.class);
	
	private static final String POST = "POST";
	private static final String GET = "GET";
	
    public static String decurtSlash(String geoserverURL) {
        if (geoserverURL.endsWith("/")) {
            geoserverURL = decurtSlash(geoserverURL.substring(0, geoserverURL.length() - 1));
        }
        return geoserverURL;
    }

	public static String post(String sUrl, String xml, String geoserverUsername, String geoserverPassword) {

		String response = null;
		HttpURLConnection connection = null;
		OutputStream os = null;

		try {
			URL url = new URL(sUrl);

			connection =
					(HttpURLConnection) url.openConnection();

			String userpass = geoserverUsername + ":" + geoserverPassword;
			@SuppressWarnings("restriction")
			String basicAuth = "Basic " + new String(new sun.misc.BASE64Encoder().encode(userpass.getBytes()));
			connection.setRequestProperty ("Authorization", basicAuth);

			connection.setDoOutput(true);
			connection.setRequestMethod(POST);
			connection.setRequestProperty("Content-Type", "application/xml");
			connection.setRequestProperty("Accept", "application/xml");

			os = connection.getOutputStream();
			os.write(xml.getBytes());

			if (connection.getResponseCode() != HttpURLConnection.HTTP_CREATED)
				LOGGER.error("Failed : HTTP error code : " + connection.getResponseCode());
			else {
				BufferedReader br = new BufferedReader(new InputStreamReader(
						(connection.getInputStream())));

				String output;
				LOGGER.info("post [" + sUrl +"]: response: " + connection.getResponseCode());
				while ((output = br.readLine()) != null) {
					LOGGER.info("Response from GeoServer: " + output);
				}
			}

			response = connection.getResponseMessage();
		}
		catch (IOException e) {
			LOGGER.error(e.getMessage());
		}
		finally {
			try {
				if (os != null) {
					os.flush();
					os.close();
				}
			} 
			catch (IOException e) {
				LOGGER.warn(e.getMessage());
			}

			if (connection != null)
				connection.disconnect();
		}

		return response;
	}

	public static String get(String sUrl, String geoserverUsername, String geoserverPassword) {
		StringBuffer output = new StringBuffer();
		HttpURLConnection connection = null;
		OutputStream os = null;

		try {
			URL url = new URL(sUrl);

			connection =
					(HttpURLConnection) url.openConnection();

			String userpass = geoserverUsername + ":" + geoserverPassword;
			@SuppressWarnings("restriction")
			String basicAuth = "Basic " + new String(new sun.misc.BASE64Encoder().encode(userpass.getBytes()));
			connection.setRequestProperty ("Authorization", basicAuth);

			connection.setRequestMethod(GET);
			connection.setRequestProperty("Accept", "application/xml");

			BufferedReader br = new BufferedReader(new InputStreamReader(
					(connection.getInputStream())));

			LOGGER.info("get [" + sUrl +"]: response: " + connection.getResponseCode());
			String line;
			while ((line = br.readLine()) != null) {
				output.append(line);
				LOGGER.debug("Response from GeoServer: " + line);
			};
		}
		catch (IOException e) {
			LOGGER.error(e.getMessage());
		}
		finally {
			try {
				if (os != null) {
					os.flush();
					os.close();
				}
			} 
			catch (IOException e) {
				LOGGER.warn(e.getMessage());
			}

			if (connection != null)
				connection.disconnect();
		}

		return output.toString();
	}
}
