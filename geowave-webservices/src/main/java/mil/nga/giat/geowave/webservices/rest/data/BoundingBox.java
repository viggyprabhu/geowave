package mil.nga.giat.geowave.webservices.rest.data;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class BoundingBox
{

	public BoundingBox() {
		super();
	}
	
	public BoundingBox(JSONObject json) throws JSONException {
		this();
		
		minx = json.getDouble(MINX);
		maxx = json.getDouble(MAXX);
		miny = json.getDouble(MINY);
		maxy = json.getDouble(MAXY);
		crs = json.getString(CRS);
	}
	
	public BoundingBox(double minx, double maxx, double miny, double maxy, String crs) {
		this.minx = minx;
		this.maxx = maxx;
		this.miny = miny;
		this.maxy = maxy;
		this.crs = crs;
	}
	
	public JSONObject toJSON() throws JSONException {
		JSONObject json = new JSONObject();
		
		json.put(MINX, minx);
		json.put(MAXX, maxx);
		json.put(MINY, miny);
		json.put(MAXY, maxy);
		json.put(CRS, crs);
		
		return json;
	}
	
	public String toJSONString() throws JSONException {
		return toJSON().toString();
	}

	public double getMinx() {
		return minx;
	}

	public void setMinx(
			double minx ) {
		this.minx = minx;
	}

	public double getMaxx() {
		return maxx;
	}

	public void setMaxx(
			double maxx ) {
		this.maxx = maxx;
	}

	public double getMiny() {
		return miny;
	}

	public void setMiny(
			double miny ) {
		this.miny = miny;
	}

	public double getMaxy() {
		return maxy;
	}

	public void setMaxy(
			double maxy ) {
		this.maxy = maxy;
	}

	public String getCrs() {
		return crs;
	}

	public void setCrs(
			String crs ) {
		this.crs = crs;
	}

	private double minx;
	private double maxx;
	private double miny;
	private double maxy;
	private String crs;
	
	private final static String MINX = "minx";
	private final static String MAXX = "maxx";
	private final static String MINY = "miny";
	private final static String MAXY = "maxy";
	private final static String CRS = "crs";
}
