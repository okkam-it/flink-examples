package it.okkam.flink.ckan;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;

import it.okkam.flink.ckan.model.CkanDatastoreSearch;
import it.okkam.flink.ckan.model.CkanDatastoreSearchResponse;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.ws.rs.core.MediaType;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CkanRestInputFormat extends RichInputFormat<Row, InputSplit>
    implements ResultTypeQueryable<Row> {
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(CkanRestInputFormat.class);

  private static final String CKAN_DOWNLOAD_URL_PATTERN = //
      "%s/api/action/datastore_search?resource_id=%s&offset=%s&limit=%s";

  private static final int CONNECTION_TIMEOUT = 10_000;// 10 s

  private boolean hasNext = true;
  private RowTypeInfo rowTypeInfo;
  private Serializable[][] parameterValues;

  private int fetchSize = 100_000;// default
  private String catalogUrl;
  private String resourceId;
  private String authHeader = "Authorization";
  private String authKey;
  private HashMap<String, Integer> fieldsMap;

  private transient CkanDatastoreSearch dataSlice;
  private transient Client httpClient;

  @Override
  public void openInputFormat() throws IOException {
    super.openInputFormat();
    httpClient = getCkanHttpClient();
  }

  private static Client getCkanHttpClient() {
    Client ret = Client.create();
    ret.setConnectTimeout(CONNECTION_TIMEOUT);
    return ret;
  }

  @Override
  public void closeInputFormat() throws IOException {
    super.closeInputFormat();
    httpClient.destroy();
  }

  @Override
  public void configure(Configuration parameters) {
    // do nothing here
  }

  @Override
  public void close() throws IOException {
    // do nothing here
  }

  @Override
  public TypeInformation<Row> getProducedType() {
    return rowTypeInfo;
  }

  @Override
  public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
    return cachedStatistics;
  }

  @Override
  public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
    return new DefaultInputSplitAssigner(inputSplits);
  }

  @Override
  public void open(InputSplit inputSplit) throws IOException {
    final Object[] fromTo = parameterValues[inputSplit.getSplitNumber()];
    final String splitDownloadUrl = String.format(CKAN_DOWNLOAD_URL_PATTERN, catalogUrl, resourceId,
        fromTo[0], fetchSize);
    dataSlice = getResourceData(httpClient, splitDownloadUrl, authHeader, authKey).getResult();
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("Downloading split #%s (%s)", inputSplit.getSplitNumber(),
          splitDownloadUrl));
    }
  }

  @Override
  public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
    if (parameterValues == null) {
      return new GenericInputSplit[] { new GenericInputSplit(0, 1) };
    }
    GenericInputSplit[] ret = new GenericInputSplit[parameterValues.length];
    for (int i = 0; i < ret.length; i++) {
      ret[i] = new GenericInputSplit(i, ret.length);
    }
    return ret;
  }

  @Override
  public boolean reachedEnd() throws IOException {
    return !hasNext;
  }

  @Override
  public Row nextRecord(Row row) throws IOException {
    if (!hasNext) {
      return null;
    }
    final Map<String, String> record = dataSlice.getRecords().get(0);
    dataSlice.getRecords().remove(0);
    for (Entry<String, Integer> fieldentry : fieldsMap.entrySet()) {
      String fieldName = fieldentry.getKey();
      final String strValue = record.get(fieldName);
      final Integer fieldPos = fieldentry.getValue();
      row.setField(fieldPos, getFieldValue(strValue, fieldName, fieldPos));
    }
    // update hasNext after we've read the record
    hasNext = !dataSlice.getRecords().isEmpty();
    return row;
  }

  private Object getFieldValue(String value, String fieldName, Integer fieldPos) {
    final TypeInformation<?> typeInfo = rowTypeInfo.getFieldTypes()[fieldPos];
    if (BasicTypeInfo.STRING_TYPE_INFO.equals(typeInfo)) {
      return value;
    }
    if (BasicTypeInfo.INT_TYPE_INFO.equals(typeInfo)) {
      return Integer.valueOf(value);
    }
    if (BasicTypeInfo.LONG_TYPE_INFO.equals(typeInfo)) {
      return Long.valueOf(value);
    }
    if (BasicTypeInfo.DOUBLE_TYPE_INFO.equals(typeInfo)) {
      return Double.valueOf(value);
    }
    if (BasicTypeInfo.FLOAT_TYPE_INFO.equals(typeInfo)) {
      return Float.valueOf(value);
    }
    if (BasicTypeInfo.BOOLEAN_TYPE_INFO.equals(typeInfo)) {
      return Boolean.valueOf(value);
    }
    throw new IllegalArgumentException(typeInfo + " not handled yet");
  }

  private static CkanDatastoreSearchResponse getResourceData(Client client, String dataSliceUrl,
      String authHeader, String authKey) {
    WebResource webResource = client.resource(dataSliceUrl);
    webResource.accept(MediaType.APPLICATION_JSON_TYPE);
    if (authKey == null) {
      LOG.info("Contacting UNSECURED CKAN datastore_search at {}", dataSliceUrl);
    } else {
      webResource.setProperty(authHeader, authKey);
    }
    return webResource.get(CkanDatastoreSearchResponse.class);
  }

  /**
   * Builder for a {@link CkanRestInputFormat}.
   */
  public static class CkanRestInputFormatBuilder {
    private final CkanRestInputFormat format;
    private static final int SAMPLING_SIZE = 1;

    	/**
		 * CkanRestInputFormatBuilder constructor.
		 * 
		 * @param catalogUrl The CKAN catalog/base url
		 * @param resourceId the resource to fetch
		 * @param fieldNames the dataset field names
		 */
    public CkanRestInputFormatBuilder(String catalogUrl, String resourceId, String[] fieldNames) {
      this.format = new CkanRestInputFormat();
      format.catalogUrl = removeTrailingSlash(catalogUrl);
      format.resourceId = resourceId;
      format.fieldsMap = new HashMap<>(fieldNames.length);
      for (int i = 0; i < fieldNames.length; i++) {
        format.fieldsMap.put(fieldNames[i], Integer.valueOf(i));
      }
    }

    /**
     * Sets the security parameters.
     * 
     * @param authHeader
     *          the header name to use for authentication key
     * @param authKey
     *          the authentication key
     * @return this CkanInputFormatBuilder
     */
    public CkanRestInputFormatBuilder setApiKey(String authHeader, String authKey) {
      format.authHeader = authHeader;
      format.authKey = authKey;
      return this;
    }

    public CkanRestInputFormatBuilder setRowTypeInfo(RowTypeInfo rowTypeInfo) {
      format.rowTypeInfo = rowTypeInfo;
      return this;
    }

    /**
     * Set /api/action/datastore_search result size.
     * 
     * @param fetchSize
     *          the number of elements to fetch per request
     * @return this CkanInputFormatBuilder
     */
    public CkanRestInputFormatBuilder setFetchSize(int fetchSize) {
      Preconditions.checkArgument(fetchSize == Integer.MIN_VALUE || fetchSize > 0,
          "Illegal value %s for fetchSize, has to be positive or Integer.MIN_VALUE.", fetchSize);
      format.fetchSize = fetchSize;
      return this;
    }

    /**
     * Return the instance of CkanInputFormat for the passed parameters.
     * 
     * @return this CkanInputFormat
     */
    public CkanRestInputFormat finish() {
      if (format.resourceId == null) {
        LOG.debug("No Resource id configured");
      }
      if (format.parameterValues == null) {
        LOG.debug("No input splitting configured (data will be read with parallelism 1).");
      }
      if (format.fieldsMap == null) {
        throw new IllegalArgumentException("Fields order must be known");
      }
      if (format.rowTypeInfo == null) {
        format.rowTypeInfo = new RowTypeInfo(getCkanFieldTypes());
      }
      final String sampleUrl = String.format(CKAN_DOWNLOAD_URL_PATTERN, format.catalogUrl,
          format.resourceId, 0, SAMPLING_SIZE);
      LOG.info("Contacting resource to get rowCount: {}", sampleUrl);
      final CkanDatastoreSearchResponse sample = getResourceData(getCkanHttpClient(), sampleUrl,
          format.authHeader, format.authKey);
      final int rowCount = sample.getResult().getRowCount();
      format.parameterValues = new CkanPaginator(rowCount, format.fetchSize).getParameterValues();
      LOG.info("Found {} rows for resource {} => generated {} splits using fetchSize {}", rowCount,
          format.resourceId, format.parameterValues.length, format.fetchSize);
      return format;
    }

    // TODO infer type from sample and field type
    private TypeInformation<?>[] getCkanFieldTypes() {
      LOG.debug("No rowtype found. Default to string.");
      final TypeInformation<?>[] types = new TypeInformation<?>[format.fieldsMap.size()];
      for (int i = 0; i < types.length; i++) {
        types[i] = BasicTypeInfo.STRING_TYPE_INFO;
      }
      return types;
    }

    /**
     * Returns the provided url with all trailing slash at the end removed.
     */
    private static String removeTrailingSlash(String url) {
      Preconditions.checkNotNull(url, "Invalid url (cannot be null)");
      String tempUrl = url.trim();
      while (tempUrl.endsWith("/")) {
        tempUrl = tempUrl.substring(0, tempUrl.length() - 1);
      }
      return tempUrl;
    }
  }

}
