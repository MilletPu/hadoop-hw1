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
 * Created by milletpu on 2017/3/24.
 * E-mail: pujun@cnic.cn
 *
 * The new class that consider the hash collision!
 */

public class Hw1Grp0 {
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
        return new BufferedReader(new InputStreamReader(in_stream));
    }

    /**
     * Hashed join process function.
     * @param fileR The address of file R.
     * @param fileS The address of file S.
     * @param joinR The join key of R.
     * @param joinS The join key of S.
     * @param resR The result columns of R.
     * @param resS The result columns of S.
     * @throws IOException Throw IOException
     */
    private void hashedJoin(String fileR, String fileS, int joinR, int joinS, int[] resR, int[] resS) throws IOException {
        //Write to HBase
        Logger.getRootLogger().setLevel(Level.WARN);
        System.out.println("Creating the table................");
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
        System.out.println("Table \'"+tableName+ "\' created successfully................");
        hAdmin.close();
        HTable table = new HTable(configuration,tableName);

        //Get files input
        BufferedReader R = readFileHdfs(fileR);
        BufferedReader S = readFileHdfs(fileS);

        //Create the hashed table for R.
        System.out.println("Creating hash table for file R................");
        Hashtable htR = new Hashtable();
        Hashtable htS = new Hashtable();
        String eachLineR;
        String eachLineS;
        while ((eachLineR = R.readLine())!=null) {
            String[] lineR = eachLineR.split("\\|");
            String temp = lineR[joinR];
            if(!htR.containsKey(temp)) {
                htR.put(temp, eachLineR);
            }else{
                while(htR.containsKey(temp)){
                    temp = temp + "*";
                }
                htR.put(temp, eachLineR);
            }
        }
        while ((eachLineS = S.readLine())!=null) {
            String[] lineS = eachLineS.split("\\|");
            String temp = lineS[joinS];
            if(!htS.containsKey(temp)) {
                htS.put(temp, eachLineS);
            }else{
                while(htS.containsKey(temp)){
                    temp = temp + "*";
                }
                htS.put(temp, eachLineS);
            }
        }

        //Match S join key with R hashed table.
        System.out.println("Matching file S with hash table of R by join key................");
        BufferedReader S1 = readFileHdfs(fileS);
        String eachLineS1;
        Hashtable already = new Hashtable(); //judge whether S has been dealt with
        int putt = 0;
        while ((eachLineS1 = S1.readLine())!=null) {
            String[] lineS1 = eachLineS1.split("\\|");
            int num = 0;
            String temp4R = lineS1[joinS]; //same
            if(!already.containsKey(temp4R)) {
                already.put(temp4R,0);
                while (htR.containsKey(temp4R)) {
                    String temp4S = lineS1[joinS];
                    while (htS.containsKey(temp4S)) {
                        Put put = new Put(temp4S.replaceAll("\\*", "").getBytes());
                        //Put Rn
                        for (int i = 0; i < resR.length; i++) {
                            String[] lineR = htR.get(temp4R).toString().split("\\|");
                            String Rn;
                            if (num != 0) {
                                Rn = "R" + resR[i] + "." + num;
                            } else {
                                Rn = "R" + resR[i];
                            }
                            String Rnres = lineR[resR[i]];
                            put.add("res".getBytes(), Rn.getBytes(), Rnres.getBytes());
                        }

                        //Put Sn
                        for (int j = 0; j < resS.length; j++) {
                            String[] lineS = htS.get(temp4S).toString().split("\\|");
                            String Sn;
                            if (num != 0) {
                                Sn = "S" + resS[j] + "." + num;
                            } else {
                                Sn = "S" + resS[j];
                            }
                            String Snres = lineS[resS[j]];
                            put.add("res".getBytes(), Sn.getBytes(), Snres.getBytes());
                        }
                        table.put(put);
                        putt++;
                        num++;
                        temp4S = temp4S + "*";
                    }
                    temp4R = temp4R + "*";
                }
            }
        }
        table.close();
        System.out.println("Successfully Done................");
        System.out.println(putt);
    }


    /**
     * Main function.
     * @param args R=/fileRAddress S=/fileSAddress join:Rx=Sy res:Ri,Rj,Sm,Sn
     * @throws IOException Throw IOException
     */
    public static void main(String[] args) throws IOException {
        System.out.println("Parsing the input command................");

        //File address of R and S
        String R, S;
        if(args[0].startsWith("R") && args[1].startsWith("S")){
            R = args[0].replace("R=", "");
            S = args[1].replace("S=", "");
        }else {
            S = args[0].replace("S=", "");
            R = args[1].replace("R=", "");
        }

        //Join key
        String join[] = args[2].replace("join:", "").split("=");
        int joinR, joinS;
        if (join[0].startsWith("R") && join[1].startsWith("S")) {
            joinR = Integer.parseInt(join[0].replaceAll("[^0-9]", ""));
            joinS = Integer.parseInt(join[1].replaceAll("[^0-9]", ""));
        }else{
            joinS = Integer.parseInt(join[0].replaceAll("[^0-9]", ""));
            joinR = Integer.parseInt(join[1].replaceAll("[^0-9]", ""));
        }

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
        for (int i = 0; i < resR.size(); i++) resRn[i] = resR.get(i);
        int[] resSn = new int[resS.size()];
        for (int i = 0; i < resS.size(); i++) resSn[i] = resS.get(i);

        //Start
        System.out.println("Starting the join process................");
        Hw1Grp0 hw1 = new Hw1Grp0();
        hw1.hashedJoin(R, S, joinR, joinS, resRn, resSn);
    }
}