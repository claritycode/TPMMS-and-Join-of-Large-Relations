import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;


public class JoinBuffer {
	ByteBuffer buffer ;
//	long channelPosition ;
//	long lastPosition ;
	FileChannel inputChannel ;

	
	public JoinBuffer ( Path f, ByteBuffer newBuffer ) {
		try {
			inputChannel = FileChannel.open(f,	EnumSet.of(StandardOpenOption.READ)) ;
		} catch (IOException e) {
			e.printStackTrace();
		}
		buffer = newBuffer ;
//		channelPosition = c ;
//		lastPosition = l ;
	}
	
	public boolean read () {
		buffer.clear () ;
		try {			
			int bytesRead = inputChannel.read(buffer) ;
			if ( bytesRead == -1 ) {
				return false ;
			}
			
		} catch ( IOException e ) {
			System.out.println ( "The following error occurred: " + e.getMessage() ) ;
			e.printStackTrace();
		}
		buffer.flip () ;
		return true ;
	}
	
	public int equalTo ( JoinBuffer rhs ) {
		int pos1 = buffer.position () ;
		int pos2 = rhs.buffer.position () ;
		for ( int k = 0; k != 7 ; ++k ) {
			if ( buffer.get(pos1 + k) < rhs.buffer.get(pos2+k)) {
				return 1 ;
			} else if ( buffer.get(pos1 + k ) > rhs.buffer.get(pos2 + k)) {
				return 2 ;
			}
		}
		return 0 ;
	}
}
