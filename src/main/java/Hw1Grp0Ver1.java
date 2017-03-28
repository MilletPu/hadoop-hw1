import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Hashtable;

/**
 * Created by milletpu on 2017/3/28.
 * E-mail: pujun@cnic.cn
 *
 * The old class that doesn't consider the hash collision! Abandon it!
 */

public class Hw1Grp0Ver1 {
    /**
     * Read files from hdfs.
     * @param fileName File name. (Take care of the address!)
     */
    private BufferedReader readFileHdfs(String fileName) throws IOException {
        String fileAdd= "hdfs://localhost:9000" + fileName;

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(fileAdd), conf);
        Path path = new Path(fileAdd);
        FSDataInputStream in_stream = fs.open(path);

        BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));

        return in;

    }


    /**
     * Hashed join process function.
     * @param fileR The address of file R.
     * @param fileS The address of file S.
     * @param joinR The join key of R.
     * @param joinS The join key of S.
     * @param resR The result columns of R.
     * @param resS The result columns of S.
     * @throws IOException
     */
    private void hashedJoin(String fileR, String fileS, int joinR, int joinS, int[] resR, int[] resS) throws IOException {
        //Write to HBase
        Logger.getRootLogger().setLevel(Level.WARN);
        // create table descriptor
        String tableName= "Result";
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
        // create column descriptor
        HColumnDescriptor cf = new HColumnDescriptor("res");
        htd.addFamily(cf);
        // configure HBase
        Configuration configuration = HBaseConfiguration.create();
        HBaseAdmin hAdmin = new HBaseAdmin(configuration);

        // If table Result exits then delete it and create a new one
        if (hAdmin.tableExists(tableName)) {
            hAdmin.disableTable(tableName);
            hAdmin.deleteTable(tableName);
        }
        hAdmin.createTable(htd);
        System.out.println("table "+tableName+ " created successfully!!");
        hAdmin.close();
        HTable table = new HTable(configuration,tableName);

        //Get files input
        BufferedReader R = readFileHdfs(fileR);
        BufferedReader S = readFileHdfs(fileS);

        //Create the hashed table for R.
        Hashtable htR = new Hashtable();
        String eachLineR;
        while ((eachLineR = R.readLine())!=null) {
            String[] lineR = eachLineR.split("\\|");
            htR.put(lineR[joinR], eachLineR);
        }

        //Match S join key with R hashed table.
        String eachLineS;
        while ((eachLineS = S.readLine())!=null) {
            String[] lineS = eachLineS.split("\\|");
            if(htR.containsKey(lineS[joinS])){
                //Put Rn
                for (int i = 0; i < resR.length; i++) {
                    String[] lineR = htR.get(lineS[joinS]).toString().split("\\|");

                    Put put = new Put(lineR[joinR].getBytes());
                    String Rn = "R" + resR[i];
                    String Rnres = lineR[resR[i]];
                    put.add("res".getBytes(), Rn.getBytes(), Rnres.getBytes());
                    table.put(put);
                    System.out.println("put successfully R");
                }

                //Put Sn
                for (int j = 0; j < resS.length ; j++) {
                    Put put = new Put(lineS[joinS].getBytes());
                    String Sn = "S" + resS[j];
                    String Snres = lineS[resS[j]];
                    put.add("res".getBytes(), Sn.getBytes(), Snres.getBytes());
                    table.put(put);
                    System.out.println("put successfully S");

                }
            }
        }

        table.close();

    }


    /**
     * Main function.
     * @param args R=/fileRAddress S=/fileSAddress join:Rx=Sy res:Ri,Rj,Sm,Sn
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        Hw1Grp0Ver1 hw1 = new Hw1Grp0Ver1();

        //File address of R and S
        String R = args[0].replace("R=", "");
        String S = args[1].replace("S=", "");

        //Join key
        String join[] = args[2].replace("join:", "").split("=");
        int joinR = Integer.parseInt(join[0].replaceAll("[^0-9]", ""));
        int joinS = Integer.parseInt(join[1].replaceAll("[^0-9]", ""));

        //Result columns
        String res[] = args[3].replace("res:", "").split(",");
        ArrayList<Integer> resR = new ArrayList<Integer>();
        ArrayList<Integer> resS = new ArrayList<Integer>();
        for (int i = 0; i < res.length; i++) {
            if (res[i].startsWith("R")) {
                resR.add(Integer.parseInt(res[i].replaceAll("[^0-9]", "")));
            }
            if (res[i].startsWith("S")) {
                resS.add(Integer.parseInt(res[i].replaceAll("[^0-9]", "")));
            }
        }
        int[] resRn = new int[resR.size()]; //ArrayList to array
        for (int i = 0; i < resR.size(); i++) {
            resRn[i] = resR.get(i);
        }
        int[] resSn = new int[resS.size()];
        for (int i = 0; i < resS.size(); i++) {
            resSn[i] = resS.get(i);
        }

        //Start
        hw1.hashedJoin(R, S, joinR, joinS, resRn, resSn);



    }
}
