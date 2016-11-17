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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.CarteServlet;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.streaming.StreamingService;
import org.pentaho.di.streaming.util.StreamingConst;
import org.pentaho.di.streaming.www.cache.StreamingCache;
import org.pentaho.di.streaming.www.cache.StreamingCacheEntry;
import org.pentaho.di.streaming.www.cache.StreamingTimedNumberedRow;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransConfiguration;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.www.BaseHttpServlet;
import org.pentaho.di.www.CarteObjectEntry;
import org.pentaho.di.www.CartePluginInterface;
import org.pentaho.di.www.JobMap;
import org.pentaho.di.www.SlaveServerDetection;
import org.pentaho.di.www.SocketRepository;
import org.pentaho.di.www.TransformationMap;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;

/**
 * This servlet allows a user to list all streaming services
 * 
 * @author matt
 *
 */
@CarteServlet(
  id = "listStreaming",
  name = "List streaming services",
  description = "List all the available streaming services" )
public class ListStreamingServicesServlet extends BaseHttpServlet implements CartePluginInterface {

  private static final long serialVersionUID = 9141152261185446007L;

  public static final String CONTEXT_PATH = "/kettle/listStreaming";

  protected boolean initialized;

  public ListStreamingServicesServlet() {
    initialized = false;
  }

  public ListStreamingServicesServlet( TransformationMap transformationMap, JobMap jobMap ) {
    super( transformationMap, jobMap );
    initialized = false;
  }

  @Override
  public void setup( TransformationMap transformationMap, JobMap jobMap, SocketRepository socketRepository, List<SlaveServerDetection> detections ) {
    super.setup( transformationMap, jobMap, socketRepository, detections );
    setupStreaming();
  }

  /**
   * <li> Start pre-loaded transformations and cache management timer
   * 
   */
  protected synchronized void setupStreaming() {
    if ( initialized ) {
      return;
    }
    initialized = true;

    try {
      final Repository repository = transformationMap.getSlaveServerConfig().getRepository(); // loaded lazily
      final IMetaStore metaStore = transformationMap.getSlaveServerConfig().getMetaStore();
      final MetaStoreFactory<StreamingService> rtFactory = new MetaStoreFactory<StreamingService>( StreamingService.class, metaStore, PentahoDefaults.NAMESPACE );

      TimerTask startPreloaded = new TimerTask() {
        public void run() {
          try {
            List<StreamingService> services = rtFactory.getElements();
            List<CarteObjectEntry> transformationObjects = transformationMap.getTransformationObjects();

            for ( StreamingService service : services ) {
              if ( service.isPreloaded() ) {
                boolean found = false;
                for ( CarteObjectEntry entry : transformationObjects ) {
                  Trans trans = transformationMap.getTransformation( entry );
                  if ( trans.isRunning() ) {
                    String serviceName = trans.getTransMeta().getAttribute( StreamingConst.REALTIME_GROUP, StreamingConst.REALTIME_SERVICE_NAME );
                    if ( serviceName != null && service.getName().equals( serviceName ) ) {
                      // This streaming service transformation is already running...
                      found = true;
                    }
                  }
                }

                if ( !found ) {
                  // Start the transformation...
                  //
                  startTransformation( repository, metaStore, service );
                }
              }
            }
          } catch ( Exception e ) {
            LogChannel.GENERAL.logError( "Error starting preloaded streaming serivices", e );
          }
        }
      };
      Timer timer = new Timer( "Streaming preload verification" );
      timer.schedule( startPreloaded, 0, 10000 );

    } catch ( Exception e ) {
      throw new RuntimeException( "Unable to setup streaming services", e );
    }
  }

  protected void startTransformation( Repository repository, IMetaStore metaStore, final StreamingService service ) {
    try {

      final StreamingCache cache = StreamingCache.getInstance();

      TransMeta transMeta = StreamingConst.loadTransMeta( repository, metaStore, service );
      Trans trans = new Trans( transMeta );
      String carteObjectId = UUID.randomUUID().toString();
      trans.setContainerObjectId( carteObjectId );
      TransExecutionConfiguration transExecutionConfiguration = new TransExecutionConfiguration();
      TransConfiguration transConfiguration = new TransConfiguration( transMeta, transExecutionConfiguration );
      transformationMap.addTransformation( transMeta.getName(), carteObjectId, trans, transConfiguration );
      trans.prepareExecution( null );

      final int maxSize = Const.toInt( transMeta.environmentSubstitute( service.getCacheSize() ), -1 );
      final int maxTime = Const.toInt( transMeta.environmentSubstitute( service.getCacheDuration() ), -1 );

      // Which step are we listening to?
      //
      StepInterface stepInterface = trans.findStepInterface( service.getStepname(), 0 );
      stepInterface.addRowListener( new RowAdapter() {
        @Override
        public void rowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {
          StreamingCacheEntry cacheEntry = cache.get( service.getName() );
          if ( cacheEntry == null ) {
            List<StreamingTimedNumberedRow> list = Collections.synchronizedList( new LinkedList<StreamingTimedNumberedRow>() );
            cacheEntry = new StreamingCacheEntry( rowMeta, list );
            cache.put( service.getName(), cacheEntry );
          }
          long now = System.currentTimeMillis();
          long id = cache.nextValue( service.getName() );
          List<StreamingTimedNumberedRow> rowList = cacheEntry.getRowData();
          rowList.add( new StreamingTimedNumberedRow( id, row ) );
          if ( maxSize > 0 && rowList.size() > maxSize ) {
            rowList.remove( 0 );
          }
          if ( maxTime > 0 ) {
            long cutOff = now - maxTime * 1000;
            Iterator<StreamingTimedNumberedRow> iterator = rowList.iterator();
            while ( iterator.hasNext() ) {
              StreamingTimedNumberedRow next = iterator.next();
              if ( next.getTime() < cutOff ) {
                iterator.remove();
              } else {
                break;
              }
            }
          }
        }
      } );

      trans.startThreads();
      // This transformation routinely never ends so we won't wait for it...
    } catch ( Exception e ) {
      throw new RuntimeException( "Unable to start transformation for streaming service '" + service.getName() + "'", e );
    }
  }

  public void doPut( HttpServletRequest request, HttpServletResponse response ) throws ServletException, IOException {
    doGet( request, response );
  }

  @SuppressWarnings( "unchecked" )
  public void doGet( HttpServletRequest request, HttpServletResponse response ) throws ServletException, IOException {
    if ( isJettyMode() && !request.getContextPath().startsWith( CONTEXT_PATH ) ) {
      return;
    }

    response.setStatus( HttpServletResponse.SC_OK );

    response.setContentType( "application/json" );
    response.setCharacterEncoding( Const.XML_ENCODING );

    IMetaStore metaStore = transformationMap.getSlaveServerConfig().getMetaStore();

    List<StreamingService> streamingServices = new ArrayList<StreamingService>();

    // Add possible services from the repository...
    //
    Repository repository = null;
    try {
      repository = transformationMap.getSlaveServerConfig().getRepository(); // loaded lazily
      MetaStoreFactory<StreamingService> rtFactory = new MetaStoreFactory<StreamingService>( StreamingService.class, metaStore, PentahoDefaults.NAMESPACE );
      List<StreamingService> rtServices = rtFactory.getElements();
      for ( StreamingService rtService : rtServices ) {
        if ( !Const.isEmpty( rtService.getName() ) && !Const.isEmpty( rtService.getStepname() ) ) {

          rtService.lookupTransObjectId( repository );
          if ( !Const.isEmpty( rtService.getTransFilename() ) || rtService.getTransObjectId() != null ) {
            if ( Const.isEmpty( rtService.getStepname() ) ) {
              log.logError( "A streaming service without a stepname specification was found : '" + rtService.getName() + "'" );
            } else if ( Const.isEmpty( rtService.getName() ) ) {
              log.logError( "A streaming service without a name was found'" );
            } else {
              streamingServices.add( rtService );
            }
          } else {
            log.logError( "The transformation specification for streaming service '" + rtService.getName() + "' could not be found" );
          }
        }
      }
    } catch ( Exception e ) {
      log.logError( "Unable to list extra repository services", e );
    }

    JSONObject json = new JSONObject();
    JSONArray jServices = new JSONArray();
    json.put( "services", jServices );

    for ( StreamingService service : streamingServices ) {

      JSONObject jService = new JSONObject();
      jServices.add( jService );
      jService.put( "name", service.getName() );

      // Also include the row layout of the service step.
      //
      try {
        TransMeta transMeta = StreamingConst.loadTransMeta( repository, metaStore, service );

        RowMetaInterface serviceFields = transMeta.getStepFields( service.getStepname() );

        JSONArray jFields = new JSONArray();

        jService.put( "fields", jFields );

        for ( ValueMetaInterface valueMeta : serviceFields.getValueMetaList() ) {
          JSONObject jField = new JSONObject();
          jFields.add( jField );
          jField.put( "name", valueMeta.getName() );
          jField.put( "type", valueMeta.getTypeDesc() );
          jField.put( "length", valueMeta.getLength() );
          jField.put( "precision", valueMeta.getPrecision() );
        }
      } catch ( Exception e ) {
        // Don't include details
        log.logError( "Unable to get fields for service " + service.getName() );
      }

    }

    response.getWriter().println( json.toJSONString() );
  }

  public String toString() {
    return "List streaming services";
  }

  public String getService() {
    return CONTEXT_PATH + " (" + toString() + ")";
  }

  public String getContextPath() {
    return CONTEXT_PATH;
  }

}
