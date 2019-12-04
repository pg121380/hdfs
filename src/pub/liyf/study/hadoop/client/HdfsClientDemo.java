package pub.liyf.study.hadoop.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
//        fileSystem.copyToLocalFile(new Path("/passwd"), new Path("/"));
//        fileSystem.close();
        long defaultBlockSize = fileSystem.getDefaultBlockSize(new Path("/"));
        fileSystem.listStatus(new Path("/"));
        System.out.println(defaultBlockSize);
        fileSystem.close();
    }
}
