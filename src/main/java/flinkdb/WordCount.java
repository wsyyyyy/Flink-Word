package flinkdb;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordCount {

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
        //String filePath = "/Users/sub/Desktop/Flink/四六级词频实验/近五年六级/2019年6月.txt";
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
                ;
        long s3 = System.currentTimeMillis();
        // execute and print result
        counts.writeAsText("/Users/sub/Desktop/Flink/四六级词频实验/六级词频/2013-2019六级总词频.txt ").setParallelism(1);
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
                if (token.length() > 1
                        && !HasDigit(token) && !token.equals("the") && !token.equals("and")
                        && !token.equals("or") && !token.equals("but") && !token.equals("so")
                        && !token.equals("you")  && !token.equals("he") && !token.equals("it")
                        && !token.equals("she") && !token.equals("they") && !token.equals("an")
                        && !token.equals("we") && !token.equals("their") && !token.equals("many")
                        && !token.equals("up") && !token.equals("too") && !token.equals("them")
                        && !token.equals("more") && !token.equals("there") && !token.equals("your")
                        && !token.equals("our") && !token.equals("out") && !token.equals("would")
                        && !token.equals("will") && !token.equals("does") && !token.equals("then")
                        && !token.equals("me") && !token.equals("most") && !token.equals("may")
                        && !token.equals("his") && !token.equals("her") && !token.equals("each")
                        && !token.equals("just") && !token.equals("also") && !token.equals("were")
                        && !token.equals("should") && !token.equals("if") && !token.equals("no")
                        && !token.equals("some") && !token.equals("now") && !token.equals("well")
                        && !token.equals("is") && !token.equals("am") && !token.equals("are")
                        && !token.equals("of") && !token.equals("to") && !token.equals("in")
                        && !token.equals("that") && !token.equals("this") && !token.equals("be")
                        && !token.equals("one") && !token.equals("two") && !token.equals("three")
                        && !token.equals("part") &&!token.equals("questions") && !token.equals("passage")
                        && !token.equals("what") && !token.equals("which") && !token.equals("who")
                        && !token.equals("where") && !token.equals("when") && !token.equals("why")
                        && !token.equals("on") && !token.equals("at") && !token.equals("with")
                        && !token.equals("as") && !token.equals("about") && !token.equals("by")
                        && !token.equals("than") && !token.equals("was") && !token.equals("not")
                        && !token.equals("for") && !token.equals("had") && !token.equals("has")
                        && !token.equals("been") && !token.equals("can") && !token.equals("etc")
                        && !token.equals("its") && !token.equals("do") && !token.equals("how")
                        && !token.equals("from") && !token.contains("_") && !token.equals("answer")
                        &&!token.equals("my") &&!token.equals("re") &&!token.equals("section")
                        && !token.matches("[\u4E00-\u9FA5]+")){
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }
}

