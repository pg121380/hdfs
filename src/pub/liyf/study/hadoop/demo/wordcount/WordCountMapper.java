package pub.liyf.study.hadoop.demo.wordcount;

public class WordCountMapper implements Mapper {
    @Override
    public void map(String line, Context context) {
        String[] words = line.split(" ");
        for(String word:words){
            Object value = context.get(word);
            if(value == null){
                context.set(word, 1);
            } else {
                int newValue = (int)value + 1;
                context.set(word, newValue);
            }
        }
    }
}
