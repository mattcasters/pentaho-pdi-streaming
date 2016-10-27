package org.pentaho.di.streaming.util;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.streaming.StreamingService;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.metastore.api.IMetaStore;

public class StreamingConst {
  public static final String REALTIME_GROUP = "Streaming";
  public static final String REALTIME_SERVICE_NAME = "StreamingServiceName";

  public static TransMeta loadTransMeta( Repository repository, IMetaStore metaStore, StreamingService service ) throws KettleException {
    TransMeta transMeta = null;
    if ( repository != null && service.getTransObjectId() != null ) {
      transMeta = repository.loadTransformation( new StringObjectId( service.getTransObjectId() ), null );
    } else if ( repository != null && service.getName() != null ) {
      String path = "/";
      String name = service.getName();
      int lastSlashIndex = service.getName().lastIndexOf( '/' );
      if ( lastSlashIndex >= 0 ) {
        path = service.getName().substring( 0, lastSlashIndex + 1 );
        name = service.getName().substring( lastSlashIndex + 1 );
      }
      RepositoryDirectoryInterface tree = repository.loadRepositoryDirectoryTree();
      RepositoryDirectoryInterface rd = tree.findDirectory( path );
      if ( rd == null )
        rd = tree; // root
      transMeta = repository.loadTransformation( name, rd, null, true, null );
    } else {
      transMeta = new TransMeta( service.getTransFilename() );
    }

    transMeta.setRepository( repository );
    transMeta.setMetaStore( metaStore );

    return transMeta;
  }
}
