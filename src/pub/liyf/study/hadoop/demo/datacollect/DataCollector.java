package pub.liyf.study.hadoop.demo.datacollect;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimerTask;
import java.util.UUID;

public class DataCollector extends TimerTask {
    @Override
    public void run() {
        //获取当前日期
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd:HH");
        String time = simpleDateFormat.format(new Date());

        File logSrcDir = new File("D:\\hadoop_demo\\logs\\accesslog");
        File[] files = logSrcDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.startsWith("access.log.");
            }
        });
        File uploadFileDir = new File("D:\\hadoop_demo\\logs\\toupload");
        for(File file:files){
            file.renameTo(new File("D:\\hadoop_demo\\logs\\toupload"));
        }

        //构造hdfs客户端对象
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "2");
        conf.set("dfs.blocksize", "64m");
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(URI.create("hdfs://192.168.249.3:9000/"), conf, "root");
            File[] uploadFiles = uploadFileDir.listFiles();
            //检查日期目录是否存在
            if(!fileSystem.exists(new Path("/logs/" + time))){
                fileSystem.mkdirs(new Path("/logs/" + time));
            }

            for(File file:uploadFiles){
                fileSystem.copyFromLocalFile(new Path(file.getAbsolutePath()), new Path("/logs/" + time + "/access_log_" + UUID.randomUUID() + ".log"));
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

    }
}
