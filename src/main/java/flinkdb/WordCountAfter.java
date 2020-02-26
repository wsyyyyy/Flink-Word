package flinkdb;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordCountAfter {

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // get input data
		/*DataSet<String> text = env.fromElements(


				"To be, or not to be,--that is the question:--",
				"Whether 'tis nobler in the mind to suffer",
				"The slings and arrows of outrageous fortune",
				"Or to take arms against a sea of troubles,happy a  request ",
				"To be, or not to be,--that is the question:--",
				"Whether 'tis nobler in the mind to suffer",
				"The slings and arrows of outrageous fortune",
				"Or to take arms against a sea of troubles,happy a  request ",
				"To be, or not to be,--that is the question:--",
				"Whether 'tis nobler in the mind to suffer",
				"The slings and arrows of outrageous fortune",
				"Or to take arms against a sea of troubles,happy a  request ",
				"To be, or not to be,--that is the question:--",
				"Whether 'tis nobler in the mind to suffer",
				"The slings and arrows of outrageous fortune",
				"Or to take arms against a sea of troubles,happy a  request ",
				"To be, or not to be,--that is the question:--",
				"Whether 'tis nobler in the mind to suffer",
				"The slings and arrows of outrageous fortune",
				"Or to take arms against a sea of troubles,happy a  request ",
				""
				);*/

        //create a MongodbInputFormat,using a Hadoop input format wrapper
        /*HadoopInputFormat<BSONWritable, BSONWritable> hdIf =
                new HadoopInputFormat<BSONWritable, BSONWritable>(new
                        MongoInputFormat(), BSONWritable.class, BSONWritable.class, new JobConf());
        //specify connection parameters
        hdIf.getJobConf().set("mongo.input.uri", "mongodb://localhost:27017/Flink.fs.file");
        DataSet<Tuple2<BSONWritable, BSONWritable>> input =
                env.createInput(hdIf);
        */
        long s1 = System.currentTimeMillis();
        String filePath = "/Users/sub/Desktop/120万单词.txt";
        //DataSet<String> text = env.readTextFile(String.valueOf(input));
        DataSet<String> text = env.readTextFile(filePath);
        long s2 = System.currentTimeMillis();
        DataSet<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(new LineSplitter())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .groupBy(0)
                        .sum(1)
                        .sortPartition(1, Order.DESCENDING).setParallelism(1)
                        //filter
                        .filter(keyValue -> keyValue.getField(0).toString().length() > 1
                                && !HasDigit(keyValue.getField(0)) && !keyValue.getField(0).equals("the")
                                && !keyValue.getField(0).equals("for") && !keyValue.getField(0).equals("and")
                                && !keyValue.getField(0).equals("or") && !keyValue.getField(0).equals("but")
                                && !keyValue.getField(0).equals("so") && !keyValue.getField(0).equals("you")
                                && !keyValue.getField(0).equals("he") && !keyValue.getField(0).equals("it")
                                && !keyValue.getField(0).equals("she") && !keyValue.getField(0).equals("they")
                                && !keyValue.getField(0).equals("an") && !keyValue.getField(0).equals("is")
                                && !keyValue.getField(0).equals("am") && !keyValue.getField(0).equals("are")
                                && !keyValue.getField(0).equals("of") && !keyValue.getField(0).equals("to")
                                && !keyValue.getField(0).equals("in") && !keyValue.getField(0).equals("that")
                                && !keyValue.getField(0).equals("this") && !keyValue.getField(0).equals("be")
                                && !keyValue.getField(0).equals("one") && !keyValue.getField(0).equals("two")
                                && !keyValue.getField(0).equals("three") && !keyValue.getField(0).equals("part")
                                && !keyValue.getField(0).equals("questions") && !keyValue.getField(0).equals("passage")
                                && !keyValue.getField(0).equals("what") && !keyValue.getField(0).equals("which")
                                && !keyValue.getField(0).equals("who") && !keyValue.getField(0).equals("where")
                                && !keyValue.getField(0).equals("when") && !keyValue.getField(0).equals("why")
                                && !keyValue.getField(0).equals("on") && !keyValue.getField(0).equals("at")
                                && !keyValue.getField(0).equals("with") && !keyValue.getField(0).equals("as")
                                && !keyValue.getField(0).equals("about") && !keyValue.getField(0).equals("by")
                                && !keyValue.getField(0).equals("than") && !keyValue.getField(0).equals("was")
                                && !keyValue.getField(0).equals("not") && !keyValue.getField(0).equals("how")
                                && !keyValue.getField(0).equals("from") && !keyValue.getField(0).equals("had")
                                && !keyValue.getField(0).equals("has") && !keyValue.getField(0).equals("been")
                                && !keyValue.getField(0).equals("can") && !keyValue.getField(0).equals("etc")
                                && !keyValue.getField(0).equals("its") && !keyValue.getField(0).equals("do")
                                && !keyValue.getField(0).equals("we") && !keyValue.getField(0).equals("their")
                                && !keyValue.getField(0).equals("many") && !keyValue.getField(0).equals("up")
                                && !keyValue.getField(0).equals("too") && !keyValue.getField(0).equals("them")
                                && !keyValue.getField(0).equals("more") && !keyValue.getField(0).equals("there")
                                && !keyValue.getField(0).equals("your") && !keyValue.getField(0).equals("our")
                                && !keyValue.getField(0).equals("out") && !keyValue.getField(0).equals("would")
                                && !keyValue.getField(0).equals("will") && !keyValue.getField(0).equals("does")
                                && !keyValue.getField(0).equals("then") && !keyValue.getField(0).equals("me")
                                && !keyValue.getField(0).equals("some") && !keyValue.getField(0).equals("now")
                                && !keyValue.getField(0).equals("most") && !keyValue.getField(0).equals("may")
                                && !keyValue.getField(0).equals("his") && !keyValue.getField(0).equals("her")
                                && !keyValue.getField(0).equals("each") && !keyValue.getField(0).equals("just")
                                && !keyValue.getField(0).equals("also") && !keyValue.getField(0).equals("were")
                                && !keyValue.getField(0).equals("should") && !keyValue.getField(0).equals("if")
                                && !keyValue.getField(0).equals("no") && !keyValue.getField(0).equals("answer")
                                && !keyValue.getField(0).equals("section") && !keyValue.getField(0).equals("re")
                                && !keyValue.getField(0).equals("well") && !keyValue.getField(0).equals("my")
                                && !keyValue.getField(0).toString().contains("_")
                                && !keyValue.getField(0).toString().matches("[\u4E00-\u9FA5]+"))
                ;
        long s3 = System.currentTimeMillis();
        // execute and print result
        counts.writeAsText("/Users/sub/Desktop/2018年6月六级统计.txt").setParallelism(1);
        counts.print();
        long s4 = System.currentTimeMillis();
        System.out.println("读取文件时间："+(s3-s2)+"ms");
        System.out.println("处理时间："+(s4-s3)+"ms");
        System.out.println("总时间："+(s4-s2)+"ms");

    }

    //
    // 	User Functions
    //

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into
     * multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
     */
    // 判断一个字符串是否含有数字
    private static boolean HasDigit(String content) {
        boolean flag = false;
        Pattern p = Pattern.compile(".*\\d+.*");
        Matcher m = p.matcher(content);
        if (m.matches()) {
            flag = true;
        }
        return flag;
    }

    /*private static boolean isNumeric(String str){
        Pattern pattern = Pattern.compile("[0-9]*");
        return pattern.matcher(str).matches();
    }*/

    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");
            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }
}


