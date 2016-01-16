import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;


public class SublistBuffer {
	static FileChannel channel ;
	ByteBuffer buffer ;
	long channelPosition ;
	long lastPosition ;
	
	public SublistBuffer ( ByteBuffer newBuffer, long c, long l ) {
		buffer = newBuffer ;
		channelPosition = c ;
		lastPosition = l ;
	}
	
	public boolean read ( ) {
		if ( lastPosition < channelPosition ) {
			return false ;
		}
		buffer.clear () ;
		try {
			int bytesRead = channel.read(buffer,channelPosition) ;
			
//			if ( bytesRead != buffer.capacity() ) {
//				throw new IOException ( "Could not read sublist" ) ;
//			}
			

		} catch (IOException e) {
			e.printStackTrace();
		}
		
		if ( lastPosition - channelPosition + 1 < buffer.capacity() ) {
			buffer.limit((int)(lastPosition - channelPosition + 1)) ;
			buffer.position(0) ;
		} else {
			buffer.flip() ;
		}
		
		channelPosition = channelPosition + buffer.limit() ;
		return true ;
	}
	
	public boolean lessThan ( SublistBuffer rhs ) {
		int pos1 = buffer.position () ;
		int pos2 = rhs.buffer.position () ;
		for ( int k = 0; k != 7 ; ++k ) {
			if ( buffer.get(pos1 + k) < rhs.buffer.get(pos2+k)) {
				return true ;
			} else if ( buffer.get(pos1 + k ) > rhs.buffer.get(pos2 + k)) {
				return false ;
			}
		}
		return false ;
	}
}
