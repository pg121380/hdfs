package pub.liyf.study.hadoop.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class HdfsClientDemo {
    public static void main(String[] args) throws IOException, InterruptedException {
        /**
         * Configuration参数对象的构造机制：
         *      构造对象时，会加载jar包中的默认配置xx-default.xml
         *      再加载用户配置xx-site.xml(如果有冲突 则会覆盖xx-default.xml中的内容)
         *      构造完成后，还可以用conf.set("K","v")再次覆盖用户配置文件中的参数值
         */
        Configuration conf = new Configuration();
        //指定客户端上传文件到hdfs时，需要保存的副本数为2
        conf.set("dfs.replication", "2");
        //指定本客户端上传文件到hdfs时，切块的规格(默认为128mb)
        conf.set("dfs.blocksize", "64m");
        //这是访问hdfs的客户端对象
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://192.168.249.3:9000/"), conf, "root");

        //上传文件到hdfs中
        fileSystem.copyFromLocalFile(new Path("D:/share_folder/hadoop-1.2.1-bin.tar.gz"), new Path("/"));

        fileSystem.close();
    }
    FileSystem fileSystem = null;
    @Before
    public void init() throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "2");
        conf.set("dfs.blocksize", "64m");
        fileSystem = FileSystem.get(URI.create("hdfs://192.168.249.3:9000/"), conf, "root");
    }

    /**
     * 从hdfs中下载文件到客户端本地磁盘
     */
    @Test
    public void testGet() throws IOException {
        fileSystem.copyToLocalFile(new Path("/hadoop-env.sh"), new Path("/"));
        long defaultBlockSize = fileSystem.getDefaultBlockSize(new Path("/"));
        fileSystem.listStatus(new Path("/"));
        System.out.println(defaultBlockSize);
        fileSystem.close();
    }

    /**
     * hdfs中测试改名
     */
    @Test
    public void testRename() throws IOException {
        boolean rename = fileSystem.rename(new Path("/core-site.xml"), new Path("/core-site.xml-rename"));
        if (rename){
            System.out.println("rename success!");
        }
    }

    /**
     * hdfs中创建文件夹
     */
    @Test
    public void testMKDir() throws IOException {
        boolean mkdirs = fileSystem.mkdirs(new Path("/testMKdir0/testMKdir1"));
        if (mkdirs){
            System.out.println("mkdirs success!");
            fileSystem.close();
        }
    }

    /**
     * hdfs中删除文件
     */
    @Test
    public void testRm() throws IOException{
        //bool值表示是否递归删除
        boolean delete = fileSystem.delete(new Path("/testMKdir0"), true);
        if(delete){
            System.out.println("delete success!");
            fileSystem.close();
        }
    }

    /**
     * 查询hdfs指定目录下的文件
     */
    @Test
    public void testLs() throws IOException {
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path("/"), false);
        while (iterator.hasNext()){
            System.out.println(iterator.next().getPath());
        }
        fileSystem.close();
    }

    /**
     * 查看给定目录下的文件和文件夹信息
     * @throws IOException
     */
    @Test
    public void testLs2() throws IOException{
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        for (FileStatus fileStatus:fileStatuses){
            System.out.println(fileStatus.getPath());
        }
        fileSystem.close();
    }

    /**
     * 从hdfs中直接读取文件内容
     */
    @Test
    public void testReadData() throws IOException {
        FSDataInputStream input = fileSystem.open(new Path("/writeDemo.txt"));
        byte[] buf = new byte[1024];
        input.read(buf);
        System.out.println(new String(buf, 0, buf.length));
        input.close();
        fileSystem.close();
    }

    /**
     * 测试直接通过流，向hdfs中写入数据
     */
    @Test
    public void testWriteData() throws IOException {
        FSDataOutputStream outputStream = fileSystem.create(new Path("/writeDemo.txt"), true);
        String data = "hello world!";
        outputStream.write(data.getBytes());
        outputStream.close();
        fileSystem.close();
    }
}
