package pub.liyf.study.hadoop.demo.datacollect;

import java.util.Timer;
import java.util.TimerTask;

/**
 * 数据采集设计：
 * 1. 启动一个定时任务用于定时探测日志源目录，获取需要采集的文件
 * 2.将需要采集的文件移动到待上传临时目录
 * 3.遍历各个文件，逐一上传到hdfs的目标路径，同时将传输完成的文件再移动到备份目录中
 *
 * 4.启动另一个定时目录，检查目录中的文件是否已经超出最长备份时长，如果超出就删除
 */
public class DataCollectMain {
    public static void main(String[] args) {
        Timer timer1 = new Timer();

        timer1.schedule(new DataCollector(), 0, 60 * 1000L);
    }
}
