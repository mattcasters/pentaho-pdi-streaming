package org.pentaho.di.streaming.trans;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.streaming.StreamingService;
import org.pentaho.di.streaming.util.StreamingConst;
import org.pentaho.di.streaming.www.cache.StreamingCache;
import org.pentaho.di.streaming.www.cache.StreamingCacheEntry;
import org.pentaho.di.streaming.www.cache.TimedNumberedRow;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;

@ExtensionPoint( id = "CaptureStreamingRowsExtensionPointPlugin", extensionPointId = "TransformationStartThreads", description = "Add listener to capture streaming rows" )
public class CaptureStreamingRowsExtensionPointPlugin implements ExtensionPointInterface {

  @Override
  public void callExtensionPoint( LogChannelInterface log, Object object ) throws KettleException {

    if ( !( object instanceof Trans ) ) {
      return;
    }
    Trans trans = (Trans) object;
    TransMeta transMeta = trans.getTransMeta();

    try {
      String serviceName = transMeta.getAttribute( StreamingConst.REALTIME_GROUP, StreamingConst.REALTIME_SERVICE_NAME );
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

      if ( service.isClearingOnStart() ) {
        cache.getCache().remove( service.getName() );
      }

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
            List<TimedNumberedRow> list = Collections.synchronizedList( new LinkedList<TimedNumberedRow>() );
            cacheEntry = new StreamingCacheEntry( rowMeta, list );
            cache.put( service.getName(), cacheEntry );
          }
          cacheEntry.setRowMeta( rowMeta );
          long now = System.currentTimeMillis();
          long id = cache.nextValue( service.getName() );
          List<TimedNumberedRow> rowList = cacheEntry.getRowData();
          rowList.add( new TimedNumberedRow( id, row ) );
          while ( maxSize > 0 && rowList.size() > maxSize ) {
            rowList.remove( 0 );
          }
          if ( maxTime > 0 ) {
            long cutOff = now - maxTime * 1000;
            Iterator<TimedNumberedRow> iterator = rowList.iterator();
            while ( iterator.hasNext() ) {
              TimedNumberedRow next = iterator.next();
              if ( next.getTime() < cutOff ) {
                iterator.remove();
              } else {
                break;
              }
            }
          }
        }
      } );

    } catch ( Exception e ) {
      throw new KettleException( "Unable to capture streaming rows of transformation '" + transMeta.getName() + "'", e );
    }
  }

}
