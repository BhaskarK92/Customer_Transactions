import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class accessHbaseTable{

   public static void main(String[] args) throws IOException, Exception{
   
      // Instantiating Configuration class
      Configuration config = HBaseConfiguration.create();

      // Instantiating HTable class
      @SuppressWarnings({ "resource", "deprecation" })
	HTable table = new HTable(config, "TRANSACTIONS");

      // Instantiating Get class
      Get g = new Get(Bytes.toBytes("101"));
      
      // Reading the data
      Result result = table.get(g);

      // Reading values from Result class object
      byte [] name = result.getValue(Bytes.toBytes("stats"),Bytes.toBytes("username"));

      byte [] txn = result.getValue(Bytes.toBytes("stats"),Bytes.toBytes("count_txn"));

      // Printing the values
      String user  = Bytes.toString(name);
      String count = Bytes.toString(txn);
      
      System.out.println("customer name: " + user + ",number of transactions: " + count);
   }
}
