package org.example;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;

public class Bitcask {
    private class Value{
        int file_id;
        int value_sz;
        int value_pos;
        long tstamp;

        public Value(int file_id, int value_sz, int value_pos, long tstamp) {
            this.file_id = file_id;
            this.value_sz = value_sz;
            this.value_pos = value_pos;
            this.tstamp = tstamp;
        }
    }
    private String active_file;
    private final String base_dir = "/usr/app/weather_monitoring/Bitcask_Dir/";
    private final String replica_dir = "/usr/app/weather_monitoring/Replica/";
    private int file_id ;
    private final int threshold = 86400;/** 1/10 day of records (10 * 60 * 60 * 24)**/
    private int byte_offset ;
    private int records ;
    Boolean update ;
    /**
     * Key >> {file_id, value_sz,value_pos, tstamp}
     * Note that key ranges from 1 : 10(# of stations).
     */
    HashMap<Integer, Value> keyDir;
    private FileOutputStream outStream ;
    private FileOutputStream outReplica;
    ScheduledExecutorService scheduler;

    public Bitcask() {
        makeDir(base_dir);
        makeDir(replica_dir);
        keyDir = new HashMap<>();
        File directory = new File(base_dir);
        // Get the list of files in the bitcask directory
        File[] files = directory.listFiles();
        for(int i = 0 ; i < files.length ; i++ ){
            if(files[i].getName().contains("hint")){
                System.out.println(base_dir + files[i].getName());
                keyDir = read_hint(replica_dir + files[i].getName());
                System.out.println("Yes, Recovered");
                break;
            }
        }
        file_id = files.length;
        active_file = base_dir + file_id + ".db";
        try {
            new File(active_file).createNewFile();
            new File(replica_dir + file_id + ".db").createNewFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        byte_offset = 0;
        set_file();
        scheduler = Executors.newScheduledThreadPool(1);
        update = false;
        records = 0;
    }
    private void set_file(){
        try {
            outStream = new FileOutputStream( active_file,true ) ;
            outReplica = new FileOutputStream(replica_dir + file_id + ".db", true);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
    private void writeToFile(byte[] bytes ){
        try {
            outStream.write(bytes);
            outReplica.write(bytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void update_dir(){
        try {
            FileUtils.cleanDirectory( new File(base_dir));
            new File(replica_dir + file_id + ".db").renameTo(new File(replica_dir + "1.db"));
            new File(replica_dir +"merged_0.db").renameTo(new File(replica_dir +"0.db"));
            new File(replica_dir + "merged_0_hint.db").renameTo(new File(replica_dir + "0_hint.db" ));
            active_file = base_dir + "1.db";
            HashMap<Integer, Value> key_hint = read_hint(replica_dir + "0_hint.db");
            for(Map.Entry<Integer, Value> set :
                    keyDir.entrySet()){
                if(set.getValue().file_id == file_id ){
                    set.getValue().file_id = 1;
                    keyDir.put(set.getKey(), set.getValue());
                }
            }
            for(Map.Entry<Integer, Value> set :
                    key_hint.entrySet()){
                if( !keyDir.containsKey(set.getKey()) ||
                        (keyDir.containsKey(set.getKey()) &&
                                set.getValue().tstamp >= keyDir.get(set.getKey()).tstamp) ){
                    keyDir.put(set.getKey(), set.getValue() );
                }
            }
            file_id = 1;
            set_file();
            FileUtils.copyDirectory( new File(replica_dir), new File(base_dir) );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        update =false;
    }
    public void put(int key, String value){
        if( update ){
            update_dir();
        }
       /** tstamp, key, value_sz, value **/
       Value v = new Value(file_id, value.getBytes().length, byte_offset + 16, System.currentTimeMillis());
       writeToFile(ByteBuffer.allocate(8).putLong(v.tstamp).array());
       writeToFile(ByteBuffer.allocate(4).putInt(key).array());
       writeToFile(ByteBuffer.allocate(4).putInt(v.value_sz).array());
       writeToFile(value.getBytes());
       keyDir.put(key, v); /** Update Key Dir **/
       byte_offset= byte_offset + 16 + v.value_sz;
       if(records == threshold){ // change active file
          file_id++;
          active_file =  base_dir + file_id + ".db";
           try {
               outStream.close(); // close the current output stream
               outReplica.close();
           } catch (IOException e) {
               throw new RuntimeException(e);
           }
           set_file();
          byte_offset = 0;
          records = 0;
       }
       records ++ ;
    }

    public String get(int key){
        if( update ){
            update_dir();
        }
        Value v = keyDir.get(key);
        if(v == null){
            return null;
        }
        try {
            RandomAccessFile raf = new RandomAccessFile( base_dir+ v.file_id+".db", "r");
            byte[] bytes = new byte[v.value_sz];
            raf.seek(v.value_pos);
            raf.read(bytes);
            raf.close();
            return new String(bytes);
        }
        catch (IOException e){
            System.out.println(e.getMessage());
        }

        return "Error";
    }
    private void makeDir(String dirPath){
        File outputDir = new File(dirPath);
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }
    }
    private class Bitcask_Entry{
        long tstamp;
        int key;
        int value_sz;
        String value;

        public Bitcask_Entry(long tstamp, int key, int value_sz, String value) {
            this.tstamp = tstamp;
            this.key = key;
            this.value_sz = value_sz;
            this.value = value;
        }
    }
    private Bitcask_Entry read_entry(InputStream in) throws IOException {
        byte[] tstamp = new byte[8];
        byte[] key = new byte[4];
        byte[] value_sz = new byte[4];
        in.read(tstamp);
        in.read(key);
        in.read(value_sz);
        byte[] value = new byte[ByteBuffer.wrap(value_sz).getInt()];
        in.read(value);
        Bitcask_Entry entry = new Bitcask_Entry(ByteBuffer.wrap(tstamp).getLong(), ByteBuffer.wrap(key).getInt(),
                ByteBuffer.wrap(value_sz).getInt(), new String(value));

        return entry ;
    }
    private void operate_on_file(String file,  HashMap<Integer, Bitcask_Entry> key_merge) throws IOException {
        InputStream in_a = new BufferedInputStream(new FileInputStream(replica_dir + file));
        while (in_a.available() > 0) {
            Bitcask_Entry entry = read_entry(in_a);
            if (key_merge.containsKey(entry.key)) {
                if (key_merge.get(entry.key).tstamp < entry.tstamp) {
                    key_merge.put(entry.key, entry);
                }
            }
            else{
                key_merge.put(entry.key, entry);
            }
        }
    }
    private void write_merged(String out,  HashMap<Integer, Bitcask_Entry> key_merge) throws IOException {
        new File(out ).createNewFile();
        FileOutputStream writer = new FileOutputStream(out) ;
        for(Map.Entry<Integer, Bitcask_Entry> set :
                  key_merge.entrySet()){
               Bitcask_Entry entry = set.getValue();
               writer.write(ByteBuffer.allocate(8).putLong(entry.tstamp).array());
               writer.write(ByteBuffer.allocate(4).putInt(entry.key).array());
               writer.write(ByteBuffer.allocate(4).putInt(entry.value_sz).array());
               writer.write(entry.value.getBytes());
          }
        writer.flush();
        writer.close();
    }
    private void merge_two(String file_a, String file_b, String out) throws IOException {
        HashMap<Integer, Bitcask_Entry> key_merge = new HashMap<>();
        /** tstamp, key, value_sz, value **/
         operate_on_file(file_a, key_merge);
         operate_on_file(file_b, key_merge);

         write_merged(out, key_merge);
    }


    /**
     * Merge all old data, leaving the active file untouched?!
     */
    private void merge() throws IOException {
        /** Think about doing a divide and conquer algorithm **/
        /** Think about when to merge. i.e using scheduler or when file_id reaches something (7 days of records)**/
        File directory = new File(replica_dir);
        // Get the list of files in the bitcask directory
        File[] files = directory.listFiles();
        ArrayList<String> merged_files = new ArrayList<>();
        String active_file = replica_dir + file_id + ".db";
        for(int i = 0 ; i < files.length; i++){
           if( !active_file.equals(replica_dir + files[i].getName()) && !files[i].getName().contains("hint")  ){
               merged_files.add(files[i].getName());
           }
        }
        int merged_id = 0;
        while (merged_files.size() > 1){
            int first = 0, end = merged_files.size() - 1;
            ArrayList<String> tmp_merged = new ArrayList<>();
            while (first <= end ){
                if(first == end){
                    tmp_merged.add(merged_files.get(first));
                    break;
                }
                String out = "merged_" + merged_id  + ".db";
                try {
                    merge_two(merged_files.get(first), merged_files.get(end), replica_dir + out);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                first++;
                end--;
                merged_id++;
                tmp_merged.add(out);
            }
            merged_files = tmp_merged;

        }

        /**
         * Hopefully, the last output file contains "merged_${merged_id- 1}
         * **/
        if(merged_id > 0)
            merged_id--;

        // Get the list of files in the bitcask directory
        files = directory.listFiles();
        for(int i = 0 ; i< files.length; i++){
            if( !active_file.equals( replica_dir + files[i].getName() ) &&
                    !files[i].getName().equals("merged_" + merged_id  + ".db") ){
                files[i].delete();
            }
        }

         String last_merged = replica_dir + "merged_" + merged_id + ".db";
        /**Entry: tstamp, key, value_sz, value **/

        String hint_file = replica_dir +"merged_" + merged_id + "_hint.db";
        int offset = 0;
        InputStream in = new BufferedInputStream(new FileInputStream(last_merged));
        FileOutputStream out = new FileOutputStream(hint_file);
        new File(hint_file).createNewFile();
        while (in.available() > 0) {
            Bitcask_Entry entry = read_entry(in);
            out.write(ByteBuffer.allocate(4).putInt(entry.key).array());
            out.write(ByteBuffer.allocate(4).putInt(0).array());
            out.write(ByteBuffer.allocate(4).putInt(entry.value_sz).array());
            out.write(ByteBuffer.allocate(4).putInt(offset + 16).array());
            out.write(ByteBuffer.allocate(8).putLong(entry.tstamp).array());
            offset = offset + 16 + entry.value_sz;
            //System.out.println(offset);
        }
        new File(last_merged).renameTo(new File(replica_dir +"merged_0.db"));
        new File(hint_file).renameTo(new File(replica_dir + "merged_0_hint.db" ));

    }
    private  HashMap<Integer, Value> read_hint(String hint_file){
        HashMap<Integer, Value> key_hint= new HashMap<>();
        try {
            InputStream in = new BufferedInputStream(new FileInputStream(hint_file));
            while ( in.available() > 0 ){
                byte[] key = new byte[4];
                byte[] file_id = new byte[4];
                byte[] value_sz = new byte[4];
                byte[] value_pos = new byte[4];
                byte[] tstamp = new byte[8];
                in.read(key);
                in.read(file_id);
                in.read(value_sz);
                in.read(value_pos);
                in.read(tstamp);
                key_hint.put( ByteBuffer.wrap(key).getInt(), new Value(ByteBuffer.wrap(file_id).getInt(),
                        ByteBuffer.wrap(value_sz).getInt(), ByteBuffer.wrap(value_pos).getInt(),
                        ByteBuffer.wrap(tstamp).getLong()) );
            }
        } catch (IOException e) {
            throw new RuntimeException(e + "_ Read Key Dir from hint files");
        }
        return key_hint;

    }

    /**
     * Scheduler thread for compaction starts after 1 day from invocation
     * occurs periodically after 1 day
     */
    public void run_compaction(){
        scheduler.scheduleAtFixedRate(new Task(),1,1, TimeUnit.DAYS);
    }
    private class Task implements Runnable {
        public void run()
        {
            System.out.println("Merging Started.....");
            try {
                merge();
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("Merging Finished....");
            update = true;
            //System.out.println(get(0));

        }
    }

    /**
     * To shut down the compaction scheduler thread
     */
    public void shut_down(){
        scheduler.shutdown();
        System.out.println("Compaction Task is shut down");
    }


}
