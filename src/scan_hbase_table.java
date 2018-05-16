import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;


public class scan_hbase_table{

   public static void main(String args[]) throws IOException{

      // Instantiating Configuration class
      Configuration config = HBaseConfiguration.create();

      // Instantiating HTable class
      @SuppressWarnings({ "deprecation", "resource" })
	 
      HTable table = new HTable(config, "TRANSACTIONS");

      // Instantiating the Scan class
      Scan scan = new Scan();

      // Scanning the required columns
      scan.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("count_txn"));
      scan.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("username"));

      // Getting the scan result
      ResultScanner scanner = table.getScanner(scan);

      // Reading values from scan result
      for (Result result = scanner.next(); result != null; result = scanner.next())

      {
    	   //assign row values in variable Row   
    	  String Row = Bytes.toString(result.getRow());
    	  
          //assign column username values in name
    	  String name = Bytes.toString(result.getValue("stats".getBytes(),"username".getBytes()));
    	  
    	  //assign column count_txn values in count
    	  String count = Bytes.toString(result.getValue("stats".getBytes(),"count_txn".getBytes()));
    	  
          System.out.println( Row + "," + name + "," + count );

       //closing the scanner
        scanner.close();
      
       }
}
}
