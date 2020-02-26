package MongoSave;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.bson.types.ObjectId;
import java.io.File;
import java.io.InputStream;
import java.util.Date;

public class Mongosave {
    private static Mongo mg = null;
    private static DB db = null;
    private static GridFS myFS = null;
    private static String oid = null;
    @SuppressWarnings("deprecation")
    public Mongosave(String ip,int port,String dbName){
        try{
            mg = new Mongo(ip,port);
            db = mg.getDB(dbName);
            myFS = new GridFS(db);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    //查询MongoDB中的文件集合
    public void queryGridFS(){
        DBCursor cursor = myFS.getFileList();
        while(cursor.hasNext()){
            System.out.println(cursor.next());
        }
    }

    //将Mongo中的文档存储为本地文件
    public void saveGridFS(String localPath){
        try{
            File f = new File(localPath);
            GridFSInputFile inputFile = myFS.createFile(f);
            inputFile.save();
            oid = inputFile.getId().toString();
            System.out.println("Save success!");
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    //删除Mongo文件
    public void removeGridFS(String oid){
        myFS.remove(new BasicDBObject("_id",new ObjectId(oid)));
        System.out.println("Remove success!");
    }

    //读取Mongo中的文件并存储到本地
    public void readGridFS(String oid,String localPath){
        try{
            GridFSDBFile inputFile = myFS.findOne(new BasicDBObject("_id",new ObjectId(oid)));
            inputFile.writeTo(localPath);
            System.out.println("Save local path.");
        }catch (Exception e){
            System.err.println(e.getClass().getName()+": "+e.getMessage());
        }
    }

    //读取Mongo文件存储至远程
    public void readFTPGridFS(String oid,String ip,int port,String userName,String passwd,String destination) {
        try {
            GridFSDBFile inputFile = myFS.findOne((new BasicDBObject("_id", new ObjectId(oid))));
            InputStream is = inputFile.getInputStream();

            FTPClient fc = new FTPClient();
            fc.connect(ip, port);
            fc.login(userName, passwd);
            fc.setBufferSize(1024);
            fc.setFileType(FTP.BINARY_FILE_TYPE);
            fc.enterLocalPassiveMode();
            if (fc.storeFile(new String(destination.getBytes("GBK"), "iso-8859-1"), is)) {
                System.out.println("Upload success!");
            } else {
                System.out.println("Upload false!");
            }
            is.close();
            fc.logout();
            fc.disconnect();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //关闭连接
    public void closeMongo(){
        mg.close();
    }

    //测试
    public static void main(String[] args){
        Mongosave mongodb = new Mongosave("127.0.0.1",27017,"Flink");
        long startTime = new Date().getTime();//开始时间
        mongodb.saveGridFS("/Users/sub/Desktop/120万单词.txt");
        long endTime = new Date().getTime();//完成时间
        System.out.println("保存完成时间："+(endTime-startTime)+"毫秒");
        mongodb.queryGridFS();
        long startTime1 = new Date().getTime();//开始时间
        //mongodb.readGridFS(oid,"D://什么鬼报告.docx");
        //long endTime1 = new Date().getTime();//完成时间
        //System.out.println("导出完成时间："+(endTime1-startTime1)+"毫秒");
        //mongodb.removeGridFS(oid);
        mongodb.queryGridFS();
        mongodb.closeMongo();
    }
}

