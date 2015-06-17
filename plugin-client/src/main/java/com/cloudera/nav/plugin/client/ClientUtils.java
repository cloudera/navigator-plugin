package com.cloudera.nav.plugin.client;


import org.apache.commons.codec.binary.Base64;
import org.springframework.http.HttpHeaders;
/**
 * Created by Nadia.Wallace on 6/16/15.
 */
public class ClientUtils {

  public static HttpHeaders getAuthHeaders(PluginConfigurations config) {
    // basic authentication with base64 encoding
    String plainCreds = String.format("%s:%s", config.getUsername(),
        config.getPassword());
    byte[] plainCredsBytes = plainCreds.getBytes();
    byte[] base64CredsBytes = Base64.encodeBase64(plainCredsBytes);
    String base64Creds = new String(base64CredsBytes);
    HttpHeaders headers = new HttpHeaders();
    headers.add("Authorization", "Basic " + base64Creds);
    return headers;
  }

  /**
   *
   * @return url for querying all sources
   */
  public static String getSourceUrl(PluginConfigurations config, String query) {
    // form the url string to request all entities with type equal to SOURCE
    String baseNavigatorUrl = config.getNavigatorUrl();
    String entitiesUrl = joinUrlPath(baseNavigatorUrl, "entities");
    return String.format("%s?query=%s", entitiesUrl, query);
    //return String.format("%s?query=%s", entities, SOURCE_QUERY);
  }

  /**
   * @param type "entities", "relations"
   * @return url for querying entities and relations
   */
  public static String getUrl(PluginConfigurations config, String type) {
    String baseNavigatorUrl = config.getNavigatorUrl();
    String typeUrl = joinUrlPath(baseNavigatorUrl, type);
    if(type.equals("entities")) {
      return typeUrl+"/paging";
    } else { return typeUrl;}
  }

  private static String joinUrlPath(String base, String component) {
    return base + (base.endsWith("/") ? "" : "/") + component;
  }
}
