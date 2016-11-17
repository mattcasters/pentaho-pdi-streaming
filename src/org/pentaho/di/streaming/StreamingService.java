package org.pentaho.di.streaming;

import java.io.DataInputStream;
import java.net.URI;
import java.util.ArrayList;

import org.apache.commons.httpclient.Credentials;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.methods.GetMethod;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.streaming.www.cache.StreamingCacheEntry;
import org.pentaho.di.streaming.www.cache.StreamingTimedNumberedRow;
import org.pentaho.metastore.persist.MetaStoreAttribute;
import org.pentaho.metastore.persist.MetaStoreElementType;

@MetaStoreElementType( name = "Kettle Streaming Service", description = "This element type contains streaming data services" )
public class StreamingService {
  protected String name;

  @MetaStoreAttribute( key = "stepname" )
  protected String stepname;

  @MetaStoreAttribute( key = "transformation_rep_object_id" )
  protected String transObjectId; // rep: by reference (1st priority)

  @MetaStoreAttribute( key = "transformation_rep_path" )
  protected String transRepositoryPath; // rep: by name (2nd priority)

  @MetaStoreAttribute( key = "transformation_filename" )
  protected String transFilename; // file (3rd priority)

  @MetaStoreAttribute( key = "preload_cache" )
  protected boolean preloaded;

  @MetaStoreAttribute( key = "max_cache_duration" )
  protected String cacheDuration;

  @MetaStoreAttribute( key = "max_cache_size" )
  protected String cacheSize;

  @MetaStoreAttribute( key = "clear_cache_on_start" )
  protected boolean clearingOnStart;

  public StreamingService() {
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name the name to set
   */
  public void setName( String name ) {
    this.name = name;
  }

  /**
   * @return the stepname
   */
  public String getStepname() {
    return stepname;
  }

  /**
   * @param stepname the stepname to set
   */
  public void setStepname( String stepname ) {
    this.stepname = stepname;
  }

  /**
   * @return the transObjectId
   */
  public String getTransObjectId() {
    return transObjectId;
  }

  /**
   * @param transObjectId the transObjectId to set
   */
  public void setTransObjectId( String transObjectId ) {
    this.transObjectId = transObjectId;
  }

  /**
   * @return the transRepositoryPath
   */
  public String getTransRepositoryPath() {
    return transRepositoryPath;
  }

  /**
   * @param transRepositoryPath the transRepositoryPath to set
   */
  public void setTransRepositoryPath( String transRepositoryPath ) {
    this.transRepositoryPath = transRepositoryPath;
  }

  /**
   * @return the transFilename
   */
  public String getTransFilename() {
    return transFilename;
  }

  /**
   * @param transFilename the transFilename to set
   */
  public void setTransFilename( String transFilename ) {
    this.transFilename = transFilename;
  }

  /**
   * @return the preloaded
   */
  public boolean isPreloaded() {
    return preloaded;
  }

  /**
   * @param preloaded the preloaded to set
   */
  public void setPreloaded( boolean preloaded ) {
    this.preloaded = preloaded;
  }

  /**
   * @return the cacheDuration
   */
  public String getCacheDuration() {
    return cacheDuration;
  }

  /**
   * @param cacheDuration the cacheDuration to set
   */
  public void setCacheDuration( String cacheDuration ) {
    this.cacheDuration = cacheDuration;
  }

  /**
   * @return the cacheSize
   */
  public String getCacheSize() {
    return cacheSize;
  }

  /**
   * @param cacheSize the cacheSize to set
   */
  public void setCacheSize( String cacheSize ) {
    this.cacheSize = cacheSize;
  }

  /**
   * Try to look up the transObjectId for transformation which are referenced by path 
   * @param repository The repository to use.
   * @throws KettleException
   */
  public void lookupTransObjectId( Repository repository ) throws KettleException {
    if ( repository == null )
      return;

    if ( Const.isEmpty( transFilename ) && transObjectId == null && !Const.isEmpty( transRepositoryPath ) ) {
      // see if there is a path specified to a repository name
      //
      String path = "/";
      String name = transRepositoryPath;
      int lastSlashIndex = name.lastIndexOf( '/' );
      if ( lastSlashIndex >= 0 ) {
        path = transRepositoryPath.substring( 0, lastSlashIndex + 1 );
        name = transRepositoryPath.substring( lastSlashIndex + 1 );
      }
      RepositoryDirectoryInterface tree = repository.loadRepositoryDirectoryTree();
      RepositoryDirectoryInterface rd = tree.findDirectory( path );
      if ( rd == null )
        rd = tree; // root

      ObjectId transformationID = repository.getTransformationID( name, rd );
      transObjectId = transformationID == null ? null : transformationID.getId();
    }
  }

  /**
   * @return the clearingOnStart
   */
  public boolean isClearingOnStart() {
    return clearingOnStart;
  }

  /**
   * @param clearingOnStart the clearingOnStart to set
   */
  public void setClearingOnStart( boolean clearingOnStart ) {
    this.clearingOnStart = clearingOnStart;
  }

  /**
   * Get the remote cache entry for the service
   * 
   * @param serviceName
   * @param username
   * @param password
   * @param baseUrl
   * @return
   */
  public StreamingCacheEntry getStreamingCache(LogChannelInterface log, String serviceName, String username, String password, String baseUrl) throws KettleException {
    
    StreamingCacheEntry entry = new StreamingCacheEntry();
    
    String url = baseUrl+"?service="+serviceName+"&binary=true";
    HttpClient client = new HttpClient();
    URI uri = null;
    try {
      uri = new URI(url);
    } catch(Exception e) {
      throw new KettleException("Unable to parse URL : '"+url+"'", e);
    }
      
    GetMethod getMethod = new GetMethod(url);
    getMethod.setDoAuthentication(true);
    
    try {
      
      // Authenticate
      //
      Credentials credentials = new UsernamePasswordCredentials(username, password);
      AuthScope authScope = new AuthScope(uri.getHost(), uri.getPort());
      client.getState().setCredentials(authScope, credentials);
      
      int responseCode = client.executeMethod(getMethod);
      log.logBasic("Received response ["+responseCode+"] from url '"+url+"'");
      if (responseCode!=200) {
        throw new KettleException("Unable to get data from streaming service '"+serviceName+"', response ["+responseCode+"] on URL: '"+url+"'");
      }
      
      // Start reading...
      //
      DataInputStream dis = new DataInputStream(getMethod.getResponseBodyAsStream());
      
      // Read the metadata
      //
      RowMetaInterface rowMeta = new RowMeta(dis);
      log.logBasic("Row metadata read: "+rowMeta.toString());
      
      entry.setRowMeta(rowMeta);
      
      // Number of rows...
      //
      int nrRows = dis.readInt();
      log.logBasic("Expected nr of rows: "+nrRows);
      
      // Read the rows...
      //
      entry.setRowData(new ArrayList<StreamingTimedNumberedRow>());
      for (int i=0;i<nrRows;i++) {
        // The id
        //
        long id = dis.readLong();
        long time = dis.readLong();
        
        // The row data
        //
        Object[] rowData = rowMeta.readData(dis);
        
        StreamingTimedNumberedRow row = new StreamingTimedNumberedRow(id, time, rowData);
        entry.getRowData().add(row);    
      }
      
      // The last of the stream we don't use (yet) but we just read it...
      //
      
      // The last ID and time of the cache...
      //
      dis.readLong();
      dis.readLong();
      
      // The first ID and time of the cache...
      //
      dis.readLong();
      dis.readLong();
      
      return entry;
    } catch(Exception e) {
      throw new KettleException("Unable to get data from cache service '"+serviceName+"'", e);
    } finally {
      // remove connection
      //
      getMethod.releaseConnection();
    }
  }

  
}
