package pub.liyf.study.hadoop.demo.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class HdfsWordCount {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        Properties properties = new Properties();
        properties.load(HdfsWordCount.class.getClassLoader().getResourceAsStream("job.properties"));
        Class<?> mapper_class = Class.forName(properties.getProperty("MAPPER_CLASS"));
        Mapper mapper = (Mapper) mapper_class.newInstance();
        Context context = new Context();

        //1.到hdfs中逐行读文件
        //2.对每一行进行业务处理
        //3.将处理结果放入缓存(hashmap)
        //4.将缓存中的数据输出到HDFS结果文件
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "2");
        conf.set("dfs.blocksize", "64m");
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://192.168.249.3:9000/"), conf, "root");
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path("/wordcount/input/"), false);
        while (iterator.hasNext()){
            LocatedFileStatus file = iterator.next();
            FSDataInputStream in = fileSystem.open(file.getPath());
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));

            //对每个文件逐行读取
            String line = null;
            while((line = reader.readLine()) != null){
                mapper.map(line, context);
            }
            reader.close();
            in.close();
        }


        //输出结果
        Map<Object, Object> contextMap = context.getContextMap();
        FSDataOutputStream outputStream = fileSystem.create(new Path("/wordcount/output/res.dat"));
        Set<Map.Entry<Object, Object>> entries = contextMap.entrySet();
        for (Map.Entry<Object, Object> entry:entries){
            String out = entry.getKey().toString() + ":" + entry.getValue() + "\n";
            outputStream.write(out.getBytes());
        }
        outputStream.close();

        fileSystem.close();
        System.out.println("数据统计完毕！");
    }
}
