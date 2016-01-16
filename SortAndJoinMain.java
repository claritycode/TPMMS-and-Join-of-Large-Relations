import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;


public class SortAndJoinMain {

	public static void main ( String[] args )  {

		
		Relation.blockSize = 4000 ;
		Relation.tupleSize = 100 ;
		
		InputStreamReader in = new InputStreamReader(System.in) ;
		BufferedReader b = new BufferedReader (in) ;
		System.out.println ( "Please select your memory Size" ) ;
		System.out.println ( "1.  5 MB" ) ;
		System.out.println ( "2. 10 MB") ;
		int choice = 1;
		try {
			choice = Integer.parseInt(b.readLine());
		} catch (NumberFormatException | IOException e) {
			e.printStackTrace();
		}
		if ( choice == 1 ) {
			Relation.mainMemorySize = 3000000 ;
		} else if ( choice == 2 ) {
			Relation.mainMemorySize = 6000000 ;
		} else {
			System.out.println( "You have entered a wrong choice" );
			System.exit(0) ;
		}
		
		final long startTime = System.nanoTime() ;
		
		Relation t1 = new Relation ( Paths.get("../T1.txt"), 500000 ) ;
		t1.sort(); 
		t1.merge();
		System.out.println ( "T1: Blocks Read for Sorting: " + t1.blocksRead ) ;
		System.out.println ( "T1: Blocks Written after Sorting: " + t1.blocksWritten ) ;
		
		t1 = null ;
		System.gc();
		
		Relation t2 = new Relation ( Paths.get("../T2_100.txt"), 1000000 ) ;
		
		t2.sort();
		t2.merge();
		
		System.out.println ( "T1: Blocks Read for Sorting: " + t2.blocksRead ) ;
		System.out.println ( "T1: Blocks written after Sorting: " + t2.blocksWritten ) ;
		
		t2.join();
		
		System.out.println ( " Blocks Read for Join: " + t2.joinBlocksRead ) ;
		
		final long endTime = System.nanoTime() ;
		System.out.println ( "Execution time: " + (float)( endTime - startTime ) / 1000000000 + " seconds" ) ;

	}
}
