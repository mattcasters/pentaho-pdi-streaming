/*!
* Copyright 2010 - 2013 Pentaho Corporation.  All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
*/

package org.pentaho.di.streaming.www;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.CarteServlet;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.streaming.www.cache.StreamingCache;
import org.pentaho.di.streaming.www.cache.StreamingCacheEntry;
import org.pentaho.di.streaming.www.cache.StreamingTimedNumberedRow;
import org.pentaho.di.www.BaseHttpServlet;
import org.pentaho.di.www.CartePluginInterface;
import org.pentaho.di.www.JobMap;
import org.pentaho.di.www.TransformationMap;

/**
 * This servlet allows a user to get data from a streaming service
 * 
 * @author matt
 *
 */
@CarteServlet(
  id = "getStreaming",
  name = "Get streaming data",
  description = "Retrieve data from a streaming service" )
public class GetStreamingServicesServlet extends BaseHttpServlet implements CartePluginInterface {

  private static final long serialVersionUID = 3302873728288246629L;
   
  public static final String CONTEXT_PATH = "/kettle/getStreaming";

  public GetStreamingServicesServlet() {
  }

  public GetStreamingServicesServlet( TransformationMap transformationMap, JobMap jobMap ) {
    super( transformationMap, jobMap );
  }

  public void doPut( HttpServletRequest request, HttpServletResponse response ) throws ServletException, IOException {
    doGet( request, response );
  }

  public void doGet( HttpServletRequest request, HttpServletResponse response ) throws ServletException, IOException {
    if ( isJettyMode() && !request.getContextPath().startsWith( CONTEXT_PATH ) ) {
      return;
    }

    String serviceName = request.getParameter( "service" );
    int lastSize = Const.toInt( request.getParameter( "last" ), -1 );
    int lastPeriod = Const.toInt( request.getParameter( "lastPeriod" ), -1 );
    long fromId = Const.toLong( request.getParameter( "fromId" ), -1L );
    long toId = Const.toLong( request.getParameter( "toId" ), -1L );
    int newSize = Const.toInt( request.getParameter( "new" ), -1 );
    int maxWait = Const.toInt( request.getParameter( "maxWait" ), -1 );
    String binaryOption = request.getParameter( "binary" );
    boolean binary = "y".equalsIgnoreCase(binaryOption) || "true".equalsIgnoreCase(binaryOption);
    long now = System.currentTimeMillis();

    // last=60
    // fromId=100
    // new=5
    // maxWait10

    response.setStatus( HttpServletResponse.SC_OK );

    if (binary) {
      LogChannel.GENERAL.logBasic( "Binary data asked for service '"+serviceName+"'");
      response.setContentType( "application/octet-stream" );
    } else {
      LogChannel.GENERAL.logBasic( "JSON data asked for service '"+serviceName+"'");
      response.setContentType( "application/json" );
      response.setCharacterEncoding( Const.XML_ENCODING );
    }
    
    try {

      if ( !Const.isEmpty( serviceName ) ) {

        StreamingCache cache = StreamingCache.getInstance();
        StreamingCacheEntry streamingCacheEntry = cache.get( serviceName );
        if ( streamingCacheEntry != null ) {
          log.logBasic( "Cache entry of '"+serviceName+"' found");
          
          // Now we have a cache entry for the service.
          // Let's get the rows from the cache with the given options...
          //
          List<StreamingTimedNumberedRow> rows = streamingCacheEntry.findRows( log, lastSize, lastPeriod, fromId, toId, newSize, maxWait, now );
          while ( rows == null ) {
            // Keep retrying with a one second delay until the service transformation has some rows.
            Thread.sleep( 1000 );
            rows = streamingCacheEntry.findRows( log, lastSize, lastPeriod, fromId, toId, newSize, maxWait, now );
          }
          
          LogChannel.GENERAL.logBasic( "Data export for '"+serviceName+"' found, "+rows.size()+" rows found");
          if (binary) {
            writeBinaryData(serviceName, response, streamingCacheEntry, rows);
          } else {
            writeJsonData(serviceName, response, streamingCacheEntry, rows);
          }
        }
      } else {
        String comment = "Streaming cache service '" + serviceName + "' doesn't exist";
        LogChannel.GENERAL.logError( comment );
        throw new KettleException(comment);
      }
    } catch ( Exception e ) {
      LogChannel.GENERAL.logError( "Error get streaming data for service '" + serviceName + "'", e);
      try {
        response.sendError(500, e.getMessage()+" - "+Const.getStackTracker(e));
      } catch(IOException ioe) {
        LogChannel.GENERAL.logError( "Error writing error response for service '" + serviceName + "'", ioe );
      }
    }

    
  }

  @SuppressWarnings("unchecked")
  private void writeJsonData(String serviceName, HttpServletResponse response, StreamingCacheEntry streamingCacheEntry, List<StreamingTimedNumberedRow> rows) throws IOException {
    JSONObject json = new JSONObject();
    try {
      
      // We have a bunch of rows, write it out...
      // First the metadata, then the data.
      //
      RowMetaInterface rowMeta = streamingCacheEntry.getRowMeta();
      JSONArray jMetadata = new JSONArray();
      json.put( "metadata", jMetadata );
      for ( int i = 0; i < rowMeta.size(); i++ ) {
        ValueMetaInterface valueMeta = rowMeta.getValueMeta( i );
        JSONObject jField = new JSONObject();
        jField.put( "colIndex", i );
        jField.put( "colType", valueMeta.getTypeDesc() );
        jField.put( "colName", valueMeta.getName() );
        jMetadata.add( jField );
      }

      // Now the data
      //
      JSONArray jRows = new JSONArray();
      json.put( "resultset", jRows );
      for ( StreamingTimedNumberedRow row : rows ) {
        JSONArray jRow = new JSONArray();
        jRows.add( jRow );
        for ( int i = 0; i < rowMeta.size(); i++ ) {
          ValueMetaInterface valueMeta = rowMeta.getValueMeta( i );
          String string = valueMeta.getString( row.getRow()[i] );
          jRow.add( string );
        }
      }

      // And finally the last ID in the result set...
      //
      if ( rows.size() > 0 ) {
        StreamingTimedNumberedRow lastRow = rows.get( rows.size() - 1 );
        long lastId = lastRow.getId();
        long lastTime = lastRow.getTime();
        json.put( "lastId", lastId );
        json.put( "lastTime", lastTime );

        StreamingTimedNumberedRow firstRow = rows.get( 0 );
        long firstId = firstRow.getId();
        long firstTime = firstRow.getTime();
        json.put( "firstId", firstId );
        json.put( "firstTime", firstTime );
      }
      
    } catch(Exception e) {
      LogChannel.GENERAL.logError( "Error getting streaming data for service '" + serviceName + "'", e );
      json = new JSONObject();
      json.put( "error", Const.getStackTracker( e ) );
    }
    
    response.getWriter().write(json.toJSONString());
  }

  private void writeBinaryData(String serviceName, HttpServletResponse response, StreamingCacheEntry streamingCacheEntry, List<StreamingTimedNumberedRow> rows) throws IOException {
   
    try {
      DataOutputStream dos = new DataOutputStream(response.getOutputStream());
      
      // Write the metadata
      //
      RowMetaInterface rowMeta = streamingCacheEntry.getRowMeta();
      rowMeta.writeMeta(dos);
      
      // Write the number of rows
      //
      dos.writeInt(rows.size());
      
      // Write the rows
      //
      for ( StreamingTimedNumberedRow row : rows ) {
        // The id
        //
        dos.writeLong(row.getId());
        
        // The timestamp
        //
        dos.writeLong(row.getTime());
        
        // The rest of the data
        //
        rowMeta.writeData(dos, row.getRow());
      }

      // Write the ID and time of the last row and the first row
      //
      // And finally the last ID in the result set...
      //
      if ( rows.size() > 0 ) {
        StreamingTimedNumberedRow lastRow = rows.get( rows.size() - 1 );
        long lastId = lastRow.getId();
        long lastTime = lastRow.getTime();
        dos.writeLong( lastId );
        dos.writeLong( lastTime );

        StreamingTimedNumberedRow firstRow = rows.get( 0 );
        long firstId = firstRow.getId();
        long firstTime = firstRow.getTime();
        dos.writeLong( firstId );
        dos.writeLong( firstTime );
      }
      
    } catch(Exception e) {
      LogChannel.GENERAL.logError( "Error getting streaming data for service '" + serviceName + "'", e );
      
      response.sendError(500, e.getMessage()+" - "+Const.getStackTracker(e));
    }
  }

  public String toString() {
    return "get streaming data";
  }

  public String getService() {
    return CONTEXT_PATH + " (" + toString() + ")";
  }

  public String getContextPath() {
    return CONTEXT_PATH;
  }

}
