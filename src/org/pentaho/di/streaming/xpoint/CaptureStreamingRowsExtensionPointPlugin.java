package org.pentaho.di.streaming.xpoint;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.streaming.StreamingService;
import org.pentaho.di.streaming.util.StreamingConst;
import org.pentaho.di.streaming.www.cache.StreamingCache;
import org.pentaho.di.streaming.www.cache.StreamingCacheEntry;
import org.pentaho.di.streaming.www.cache.StreamingTimedNumberedRow;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransAdapter;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;

@ExtensionPoint( 
    id = "CaptureStreamingRowsExtensionPointPlugin", 
    extensionPointId = "TransformationStartThreads", 
    description = "Add listener to capture streaming rows" 
    )
public class CaptureStreamingRowsExtensionPointPlugin implements ExtensionPointInterface {

  @Override
  public void callExtensionPoint( LogChannelInterface log, Object object ) throws KettleException {

    if ( !( object instanceof Trans ) ) {
      return;
    }
    Trans trans = (Trans) object;
    TransMeta transMeta = trans.getTransMeta();

    try {
      String serviceName = transMeta.getAttribute( StreamingConst.STREAMING_GROUP, StreamingConst.STREAMING_SERVICE_NAME );
      if ( Const.isEmpty( serviceName ) ) {
        return;
      }
      IMetaStore metaStore = transMeta.getMetaStore();
      if ( metaStore == null ) {
        throw new KettleException( "No metastore reference found in TransMeta" );
      }
      MetaStoreFactory<StreamingService> rtFactory = new MetaStoreFactory<StreamingService>( StreamingService.class, metaStore, PentahoDefaults.NAMESPACE );
      final StreamingService service = rtFactory.loadElement( serviceName );
      if ( service == null ) {
        return;
      }
      final StreamingCache cache = StreamingCache.getInstance();

      final String cacheName;
      if (service.isCacheFlipping()) {
        cacheName = service.getName()+"-temp-"+UUID.randomUUID();
        log.logBasic("Cache flipping transformation, temporary cache is : "+cacheName);
      } else {
        cacheName = service.getName();    
      }

      if ( service.isClearingOnStart() ) {
        cache.getCache().remove( cacheName );
      }
      
      final int maxSize = Const.toInt( transMeta.environmentSubstitute( service.getCacheSize() ), -1 );
      final int maxTime = Const.toInt( transMeta.environmentSubstitute( service.getCacheDuration() ), -1 );

      // Which step are we listening to?
      //
      StepInterface stepInterface = trans.findStepInterface( service.getStepname(), 0 );
      stepInterface.addRowListener( new RowAdapter() {
        @Override
        public void rowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {
          StreamingCacheEntry cacheEntry = cache.get( cacheName );
          if ( cacheEntry == null ) {
            if (log.isDebug()) {
              log.logDebug("Creating a new streaming cache for service : " + service.getName());
            }

            List<StreamingTimedNumberedRow> list = Collections.synchronizedList( new LinkedList<StreamingTimedNumberedRow>() );
            cacheEntry = new StreamingCacheEntry( rowMeta, list );
            cache.put( cacheName, cacheEntry );             
          }
          cacheEntry.setRowMeta( rowMeta );
          long now = System.currentTimeMillis();
          long id = cache.nextValue( service.getName() );
          try {
            if (log.isDebug()) {
              log.logDebug("Adding row to cache : " + rowMeta.getString(row) + " buffer size : " + cacheEntry.size());
            }
          } catch (KettleValueException e) {
            throw new KettleStepException(e);
          }
          cacheEntry.addRow(new StreamingTimedNumberedRow( id, row ));
          
          while ( maxSize > 0 && cacheEntry.size() > maxSize ) {
            cacheEntry.removeFirst();
            if (log.isDebug()) {
              log.logDebug("Removed a row from cache : new buffer size : " + cacheEntry.size());
            }
          }
          if ( maxTime > 0 ) {
            long cutOff = now - maxTime * 1000;
            Iterator<StreamingTimedNumberedRow> iterator = cacheEntry.getIterator();
            while ( iterator.hasNext() ) {
              StreamingTimedNumberedRow next = iterator.next();
              if ( next.getTime() < cutOff ) {
                iterator.remove();
              } else {
                break;
              }
            }
            if (log.isDebug()) {
              log.logDebug("Pruned by max time window ("+maxTime+") from ["+service.getCacheDuration()+"] : new buffer size : " + cacheEntry.size());
            }
          }
        }
      } );
      
      // Do we need to flip the cache at the end of the transformation?
      //
      if (service.isCacheFlipping()) {
        trans.addTransListener(new TransAdapter() {
          @Override
          public void transFinished(Trans trans) throws KettleException {
            synchronized(cache) {
              
              log.logBasic("Replacing cache for '"+serviceName+"' with cache '"+cacheName+"'");
              
              StreamingCacheEntry newCacheEntry = cache.getCache().get(cacheName);

              // Remove the old cache...
              // And the temporary one...
              //
              cache.getCache().remove(cacheName);
              
              // Replace/flip with the new cache...
              //
              cache.getCache().put(serviceName, newCacheEntry);
            }
          }
        });
      }

    } catch ( Exception e ) {
      throw new KettleException( "Unable to capture streaming rows of transformation '" + transMeta.getName() + "'", e );
    }
  }

}
