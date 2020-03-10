package flinkdb;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordCountRate {
    static double Sum = 0;
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
        String filePath = "/Users/sub/Desktop/Flink/考研词频实验/2013-2019英一/2013英一.txt";
        //String filePath = "/Users/sub/Desktop/120万单词.txt";
        //DataSet<String> text = env.readTextFile(String.valueOf(input));
        DataSet<String> text = env.readTextFile(filePath);
        long s2 = System.currentTimeMillis();
        DataSet<Tuple1<Long>> counts = text.flatMap(new LineSplitter()).sum(0);
        counts.writeAsText("/Users/sub/Desktop/w13.txt").setParallelism(1);
        //counts.print();

        String filePath1 = "/Users/sub/Desktop/w13.txt";
        BufferedReader br = null;
        try {
            //获得输入流对象，可以读取文件
            br = new BufferedReader(new FileReader(filePath1));
            String line;
            line = br.readLine();
            line = line.substring(1,line.length()-1);
            Sum = Long.parseLong(line);
        }catch(Exception e) {
            e.printStackTrace();
        }finally{
            try {
                if(null != br){
                    br.close();
                }
            }catch(IOException e) {
                e.printStackTrace();
            }
        }
        DataSet<Tuple2<String,Integer>> wordPair = text.flatMap(new Word())// group by the tuple field "0" and sum up tuple field "1"
                .groupBy(0)
                .sum(1)
                .sortPartition(1, Order.DESCENDING).setParallelism(1);
        //wordPair.print();
        DataSet<Tuple3<String,Integer,String>> result = wordPair.flatMap(new Rate()).sortPartition(1,Order.DESCENDING).setParallelism(1);
        result.print();
        long s3 = System.currentTimeMillis();
        // execute and print result
        //counts.writeAsText("/Users/sub/Desktop/Flink/考研词频实验/考研词频统计/2013-2019英一词频.txt ").setParallelism(1);
        //counts.writeAsCsv("/Users/sub/Desktop/2013-2019六级总词频.csv ").setParallelism(1);
        //counts.writeAsText("/Users/sub/Desktop/词频.txt ").setParallelism(1);
        long s4 = System.currentTimeMillis();
        //System.out.println("读取文件时间："+(s3-s2)+"ms");
        //System.out.println("处理时间："+(s4-s3)+"ms");
        //System.out.println("总时间："+(s4-s2)+"ms");

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

    public static final class LineSplitter implements FlatMapFunction<String, Tuple1<Long>> {
        @Override
        public void flatMap(String value, Collector<Tuple1<Long>> out) throws Exception {
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
                    out.collect(new Tuple1<>((long) 1));
                }
            }
        }
    }
    public static final class Word implements FlatMapFunction<String, Tuple2<String, Integer>> {
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
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
    public final static class Rate implements FlatMapFunction<Tuple2<String,Integer>, Tuple3<String, Integer,String>> { @Override
        public void flatMap(Tuple2<String, Integer> value, Collector<Tuple3<String, Integer, String>> out) throws Exception {
            String s = (value.f1 / Sum) * 100 + "%";
            out.collect(new Tuple3<>(value.f0, value.f1, s));
        }
    }
}

