
import java.io.IOException;
import java.nio.* ;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.Random;

public class Relation {
	
	static int mainMemorySize ;
	static int tupleSize ;
	static int blockSize ;
	static int o = 0;
	// Instance Variables
	FileChannel sourceRelation ;
	FileChannel outputChannel ;
	ByteBuffer outputBuffer ;
	SublistBuffer[] buffers ;
	JoinBuffer[] joinBuffers ;
	int bufferHeapSize ;
	int relationBlocks ;
	int noOfSublists ;
	int relationSize ;
	int joinTuplesPerBlock ;
	int blocksRead ;
	int blocksWritten ;
	int joinBlocksRead = 0 ;
//	int diskIO ;
	// Constructor
	public Relation ( Path file, int newRelationSize ) {
		++o ;
		blocksRead = 0 ;
		blocksWritten = 0 ;
		relationSize = newRelationSize ;
		relationBlocks = ( relationSize * tupleSize ) / blockSize ;
		noOfSublists  = relationBlocks / ( mainMemorySize / blockSize ) ;
		if ( relationBlocks % ( mainMemorySize / blockSize ) != 0) {
			++noOfSublists ;
		}
		
		try {
			sourceRelation = FileChannel.open(file, EnumSet.of(StandardOpenOption.READ)) ;
			outputBuffer = ByteBuffer.allocate(mainMemorySize) ;			
			outputChannel = FileChannel.open(Files.createFile(Paths.get("../t" + o + "sortedSublist.txt"))
					, EnumSet.of(StandardOpenOption.APPEND)) ;
		} catch ( IOException e ) {
			System.out.println ( "The following error occurred: " + e.getMessage()) ;
			e.printStackTrace();
		} 
	}
	
	private void read () {
		outputBuffer.clear () ;
		try {			
			int bytesRead = sourceRelation.read(outputBuffer) ;
			blocksRead = blocksRead + bytesRead / blockSize ;
		} catch ( IOException e ) {
			System.out.println ( "The following error occurred: " + e.getMessage() ) ;
			e.printStackTrace();
		}
		outputBuffer.flip () ;
	}
	
	private void write ()  {
		
		
		try {
			int byteWrites = outputChannel.write(outputBuffer) ;
			blocksWritten = blocksWritten + byteWrites / blockSize ;
		} catch ( IOException e ) {
			System.out.println ( "The following error occurred: " + e.getMessage() ) ;
			e.printStackTrace();
		}

		outputBuffer.clear() ;
		
	}
	
	private void quicksort ( int p, int q ) {
		if ( p < q ) {
			int r = partition ( p, q ) ;
			quicksort ( p, r - 100 ) ;
			quicksort ( r + 100, q ) ;
		}
		return ;
	}
	
	private int partition ( int p, int q ) {
		Random rand = new Random () ;
		int pivot = p + rand.nextInt( (q - p) / 100 ) * 100 ;
		exchange ( pivot, q ) ;
		int i = p - 100 ;
		for ( int j = p ; j != q ; j = j + 100 ) {
			for ( int k = 0; k != 7; ++k ) {
				if ( outputBuffer.get(j+k) < outputBuffer.get(q+k)) {
					i = i + 100 ;
					exchange (i,j) ;
					break ;
				} else if ( outputBuffer.get(j+k) > outputBuffer.get(q+k)) {
					break ;
				}
			}
		}
		i = i + 100 ;
		exchange ( i, q ) ;
		return i ;
	}
	
	private void exchange ( int i, int j ) {
		if ( i == j ) {
			return ;
		}
		
		int currentPosition = outputBuffer.position() ;
		
		outputBuffer.position (i) ;
		byte[] tempI = new byte[100] ;
		outputBuffer.get(tempI) ;
		
		outputBuffer.position(j) ;
		byte[] tempJ = new byte[100] ;
		outputBuffer.get(tempJ) ;
		
		outputBuffer.position (j) ;
		outputBuffer.put(tempI) ;
		
		outputBuffer.position (i) ;
		outputBuffer.put(tempJ) ;
		
		outputBuffer.position (currentPosition) ;
		return ;
	}
	
	
	public void sort () {
		for ( int i = 0 ; i != noOfSublists - 1; ++i ) {
				read();			
				quicksort(0, mainMemorySize - tupleSize );
				write();
		}
		
		if ( relationBlocks % ( mainMemorySize / blockSize ) != 0 ) {
				read();			
				quicksort(0,   ( ( relationBlocks % ( mainMemorySize / blockSize ) )  * blockSize ) - tupleSize );
				write();
		} else {
				read() ;
				quicksort(0, mainMemorySize - tupleSize ) ;
				write() ;
		}
		if ( sourceRelation.isOpen() && outputChannel.isOpen() ) {
			try {
				sourceRelation.close();
				outputChannel.close();
				outputBuffer = null ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void loadBuffers () {
		try {
		SublistBuffer.channel = FileChannel.open(Paths.get("../t" + o + "sortedSublist.txt"), 
				EnumSet.of(StandardOpenOption.READ)) ;
		
		outputChannel = FileChannel.open(Files.createFile(Paths.get("../t" + o +"sorted.txt")), 
				EnumSet.of(StandardOpenOption.APPEND) ) ;
		
		// Calculate Buffer Size
		int mainMemoryBlocks = mainMemorySize / blockSize ;
		int sublistBufferSize = (int)( mainMemoryBlocks * 0.7 ) / noOfSublists ;
		int outputBufferSize = (int) ( ( mainMemoryBlocks * 0.3 ) + ( ( mainMemoryBlocks * 0.7 ) 
				% noOfSublists ) ) ;
		
		buffers = new SublistBuffer [ noOfSublists ] ;
		
		// Create input buffers and perform first read
		long channelPosition ;
		long lastPosition ;
		for ( int i = 0; i != noOfSublists  - 1; ++i ) {
			channelPosition = i  * mainMemorySize  ;
			lastPosition = (i+1) * mainMemorySize - 1 ;
			buffers[i] = new SublistBuffer ( ByteBuffer.allocate(sublistBufferSize*blockSize), channelPosition, 
					lastPosition );
			buffers[i].read();
			blocksRead = blocksRead + (buffers[i].buffer.limit() - buffers[i].buffer. position() ) / blockSize ;
		}
		
		// The last of the input buffer (It can be smaller than others)
		buffers[noOfSublists-1] = new SublistBuffer (ByteBuffer.allocate(sublistBufferSize*blockSize), 
				(noOfSublists-1)*mainMemorySize, (relationSize*tupleSize - 1)) ;
		buffers[noOfSublists-1].read();
		blocksRead = blocksRead + (buffers[noOfSublists-1].buffer.limit() - buffers[noOfSublists-1].buffer. position() ) / blockSize ;
		// Create output buffer
		outputBuffer = ByteBuffer.allocate(outputBufferSize*blockSize) ;
		
	} catch ( IOException e ) {
		System.out.println ( "The following error occurred: " + e.getMessage() ) ;
	}
	}
	
	private void writeMerge()  {
		outputBuffer.flip() ;
		try {
		 int bytesWritten = outputChannel.write(outputBuffer) ;
			blocksWritten = blocksWritten + bytesWritten / blockSize ;
			if ( outputBuffer.position() != outputBuffer.limit() ) {
				throw new IOException("Output Buffer write not successfull") ;
			}
		} catch ( IOException e ) {
			System.out.println( "The following error occurred: " + e.getMessage() );
			e.printStackTrace() ;
		}
		outputBuffer.clear() ;
	}
	
	private void minHeapify ( int i ) {
		int leftChild = (2 * i) + 1 ;
		int rightChild = ((2 * i) + 1) + 1 ;
		int smallest ;
		if ( leftChild <= bufferHeapSize && buffers[leftChild].lessThan(buffers[i]) ) {
			smallest = leftChild ;
		} else {
			smallest = i ;
		}
		if ( rightChild <= bufferHeapSize && buffers[rightChild].lessThan(buffers[smallest]) ) {
			smallest = rightChild ;
		}
		if ( smallest != i ) {
			SublistBuffer temp = buffers[smallest] ;
			buffers[smallest] = buffers[i] ;
			buffers[i] = temp ;
			minHeapify ( smallest ) ;
		}
	}
	
	private void buildMinHeap () {
		bufferHeapSize = buffers.length - 1 ;
		for ( int i =  ( buffers.length / 2 ) - 1 ; i != -1 ; --i ) {
			minHeapify ( i ) ;
		}
	}
	
	private byte[] extractMin () {
		byte[] result = new byte[100] ;
		try {
			buffers[0].buffer.get(result) ;
			
		} catch ( BufferUnderflowException e ) {
		}
		
		if ( buffers[0].buffer.position() == buffers[0].buffer.limit() ) {
			if ( ! buffers[0].read() ) {
				SublistBuffer temp = buffers[0] ;
				buffers[0] = buffers[bufferHeapSize] ;
				buffers[bufferHeapSize] = temp ;
				--bufferHeapSize ;
			} else {
				blocksRead = blocksRead + ( buffers[0].buffer.limit() - buffers[0].buffer.position() ) / blockSize ;
			}
		}
		
		minHeapify (0) ;
		
		return result ;
	}
	
	public void merge () {
		loadBuffers() ;
		int outputBufferTuples = outputBuffer.capacity() / tupleSize ;
		int outputBufferRefills = relationSize / outputBufferTuples ;
		buildMinHeap() ;
		while ( outputBufferRefills != 0 ) {
			int i = outputBufferTuples ;
			while ( i != 0 ) {
				outputBuffer.put(extractMin()) ;
				--i ;
			}
			writeMerge() ;

			--outputBufferRefills ;
		}
		int j = relationSize % outputBufferTuples ;
		while ( j != 0 ) {
			outputBuffer.put(extractMin()) ;
			--j ;
		}
		writeMerge () ;
		if ( SublistBuffer.channel.isOpen() ) {
			try {
				SublistBuffer.channel.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		if ( outputChannel.isOpen() ) {
			
			try {
				outputChannel.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		
		}
		for ( int i = 0; i != buffers.length; i++ ) {
			buffers[i].buffer = null  ;
		}
		buffers = null ;
		outputBuffer = null ;
		
	}
	
	public void loadInitialsJoin () {
		try {
		
		outputChannel = FileChannel.open(Files.createFile(Paths.get("../JoinResult.txt")), 
				EnumSet.of(StandardOpenOption.APPEND) ) ;
		
		// Calculate Buffer Size
		int mainMemoryBlocks = mainMemorySize / blockSize ; // 1000
		int relationBufferSize = (int)( mainMemoryBlocks * 0.7 ) / 2 ; //350
		int outputBufferSize = (int) ( ( mainMemoryBlocks * 0.3 ) + ( ( mainMemoryBlocks * 0.7 ) 
				% 2 ) ) ; // 300
		joinTuplesPerBlock = blockSize / ( (2 *tupleSize) - 8 ) ; // 20
		joinBuffers = new JoinBuffer [ 2 ] ;
		joinBuffers[0] = new JoinBuffer ( Paths.get("../t1sorted.txt"), 
				ByteBuffer.allocate(relationBufferSize * blockSize)) ;
		

		joinBuffers[1] = new JoinBuffer ( Paths.get("../t2sorted.txt"), 
				ByteBuffer.allocate(relationBufferSize * blockSize)) ;
		outputBuffer = ByteBuffer.allocate(outputBufferSize*blockSize) ;
		joinBuffers[0].read() ;
		joinBuffers[1].read() ;
		joinBlocksRead = joinBlocksRead + (joinBuffers[0].buffer.limit() - joinBuffers[0].buffer.position() ) /blockSize ;
		joinBlocksRead = joinBlocksRead + (joinBuffers[1].buffer.limit() - joinBuffers[1].buffer.position() ) /blockSize ;
	} catch ( IOException e ) {
		System.out.println ( "The following error occurred: " + e.getMessage() ) ;
	}	
	}
	
	public void writeJoin () {
		outputBuffer.flip() ;
		try {
			int bytesWritten = outputChannel.write(outputBuffer) ;

			if ( outputBuffer.position() != outputBuffer.limit() ) {
				throw new IOException("Output Buffer write not successfull") ;
			}
		} catch ( IOException e ) {
			System.out.println( "The following error occurred: " + e.getMessage() );
			e.printStackTrace() ;
		}
		outputBuffer.clear() ;
		
	}
	
	 public void join () {
		loadInitialsJoin() ;
		int outputBufferTuples = ( outputBuffer.capacity() / blockSize ) * joinTuplesPerBlock ;
		int i ;
		int flag = 1 ;
		while ( true ) {
			i = outputBufferTuples ;
			while ( i != 0 ) {
				int result = joinBuffers[0].equalTo(joinBuffers[1]) ;
				
				if ( result == 1 ) {
					joinBuffers[0].buffer.position(joinBuffers[0].buffer.position() + tupleSize );
				}
				else if ( result == 2 ) {
					joinBuffers[1].buffer.position(joinBuffers[1].buffer.position() + tupleSize );
				}
				else {
					byte[] t1= new byte[192] ;
					joinBuffers[0].buffer.get(t1, 0, 99 ) ;
					joinBuffers[0].buffer.position ( joinBuffers[0].buffer.position() - tupleSize + 1 ) ;
					
					joinBuffers[1].buffer.position( joinBuffers[1].buffer.position() + 7 ) ;
					joinBuffers[1].buffer.get(t1, 99, 93 ) ;
					outputBuffer.put(t1) ;
					--i ;
				}
				for ( int j = 0 ; j != 2 ; ++j ) {
					if ( joinBuffers[j].buffer.position () == joinBuffers[j].buffer.limit() ) {
						if ( ! joinBuffers[j].read() ) {
							flag = 0 ;
							i = 0 ;
							break ;
						} else {
							joinBlocksRead = joinBlocksRead + (joinBuffers[j].buffer.limit() - joinBuffers[j].buffer.position() ) /blockSize ;
						}
					}	
				}
			
			}
			writeJoin () ;
			if ( flag == 0 ) {
				break ;
			}

		}
	}
}
