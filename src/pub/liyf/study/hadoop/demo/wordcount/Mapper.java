package pub.liyf.study.hadoop.demo.wordcount;

public interface Mapper {
    void map(String line, Context context);
}
