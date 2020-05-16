package flinkdb;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Properties;

/*stanfordnlp词性标注表
    ROOT：要处理文本的语句
    IP：简单从句
    NP：名词短语
    VP：动词短语
    PU：断句符，通常是句号、问号、感叹号等标点符号
    LCP：方位词短语
    PP：介词短语
    CP：由‘的’构成的表示修饰性关系的短语
    DNP：由‘的’构成的表示所属关系的短语
    ADVP：副词短语
    ADJP：形容词短语
    DP：限定词短语
    QP：量词短语
    NN：常用名词
    NR：固有名词
    NT：时间名词
    PN：代词
    VV：动词
    VC：是
    CC：表示连词
    VE：有
    VA：表语形容词
    AS：内容标记（如：了）
    VRD：动补复合词
    CD: 表示基数词
    DT: determiner 表示限定词
    EX: existential there 存在句
    FW: foreign word 外来词
    IN: preposition or conjunction, subordinating 介词或从属连词
    JJ: adjective or numeral, ordinal 形容词或序数词
    JJR: adjective, comparative 形容词比较级
    JJS: adjective, superlative 形容词最高级
    LS: list item marker 列表标识
    MD: modal auxiliary 情态助动词
    PDT: pre-determiner 前位限定词
    POS: genitive marker 所有格标记
    PRP: pronoun, personal 人称代词
    RB: adverb 副词
    RBR: adverb, comparative 副词比较级
    RBS: adverb, superlative 副词最高级
    RP: particle 小品词
    SYM: symbol 符号
    TO:”to” as preposition or infinitive marker 作为介词或不定式标记
    WDT: WH-determiner WH限定词
    WP: WH-pronoun WH代词
    WP$: WH-pronoun, possessive WH所有格代词
    WRB:Wh-adverb WH副词

    关系表示
    abbrev: abbreviation modifier，缩写
    acomp: adjectival complement，形容词的补充；
    advcl : adverbial clause modifier，状语从句修饰词
    advmod: adverbial modifier状语
    agent: agent，代理，一般有by的时候会出现这个
    amod: adjectival modifier形容词
    appos: appositional modifier,同位词
    attr: attributive，属性
    aux: auxiliary，非主要动词和助词，如BE,HAVE SHOULD/COULD等到
    auxpass: passive auxiliary 被动词
    cc: coordination，并列关系，一般取第一个词
    ccomp: clausal complement从句补充
    complm: complementizer，引导从句的词好重聚中的主要动词
    conj : conjunct，连接两个并列的词。
    cop: copula。系动词（如be,seem,appear等），（命题主词与谓词间的）连系
    csubj : clausal subject，从主关系
    csubjpass: clausal passive subject 主从被动关系
    dep: dependent依赖关系
    det: determiner决定词，如冠词等
    dobj : direct object直接宾语
    expl: expletive，主要是抓取there
    infmod: infinitival modifier，动词不定式
    iobj : indirect object，非直接宾语，也就是所以的间接宾语；
    mark: marker，主要出现在有“that” or “whether”“because”, “when”,
    mwe: multi-word expression，多个词的表示
    neg: negation modifier否定词
    nn: noun compound modifier名词组合形式
    npadvmod: noun phrase as adverbial modifier名词作状语
    nsubj : nominal subject，名词主语
    nsubjpass: passive nominal subject，被动的名词主语
    num: numeric modifier，数值修饰
    number: element of compound number，组合数字
    parataxis: parataxis: parataxis，并列关系
    partmod: participial modifier动词形式的修饰
    pcomp: prepositional complement，介词补充
    pobj : object of a preposition，介词的宾语
    poss: possession modifier，所有形式，所有格，所属
    possessive: possessive modifier，这个表示所有者和那个’S的关系
    preconj : preconjunct，常常是出现在 “either”, “both”, “neither”的情况下
    predet: predeterminer，前缀决定，常常是表示所有
    prep: prepositional modifier
    prepc: prepositional clausal modifier
    prt: phrasal verb particle，动词短语
    punct: punctuation，这个很少见，但是保留下来了，结果当中不会出现这个
    purpcl : purpose clause modifier，目的从句
    quantmod: quantifier phrase modifier，数量短语
    rcmod: relative clause modifier相关关系
    ref : referent，指示物，指代
    rel : relative
    root: root，最重要的词，从它开始，根节点
    tmod: temporal modifier
    xcomp: open clausal complement
    xsubj : controlling subject 掌控者
    中心语为谓词
      subj — 主语
     nsubj — 名词性主语（nominal subject） （同步，建设）
       top — 主题（topic） （是，建筑）
    npsubj — 被动型主语（nominal passive subject），专指由“被”引导的被动句中的主语，一般是谓词语义上的受事 （称作，镍）
     csubj — 从句主语（clausal subject），中文不存在
     xsubj — x主语，一般是一个主语下面含多个从句 （完善，有些）
    中心语为谓词或介词
       obj — 宾语
      dobj — 直接宾语 （颁布，文件）
      iobj — 间接宾语（indirect object），基本不存在
     range — 间接宾语为数量词，又称为与格 （成交，元）
      pobj — 介词宾语 （根据，要求）
      lobj — 时间介词 （来，近年）
    中心语为谓词
      comp — 补语
     ccomp — 从句补语，一般由两个动词构成，中心语引导后一个动词所在的从句(IP) （出现，纳入）
     xcomp — x从句补语（xclausal complement），不存在
     acomp — 形容词补语（adjectival complement）
     tcomp — 时间补语（temporal complement） （遇到，以前）
    lccomp — 位置补语（localizer complement） （占，以上）
           — 结果补语（resultative complement）
    中心语为名词
       mod — 修饰语（modifier）
      pass — 被动修饰（passive）
      tmod — 时间修饰（temporal modifier）
     rcmod — 关系从句修饰（relative clause modifier） （问题，遇到）
     numod — 数量修饰（numeric modifier） （规定，若干）
    ornmod — 序数修饰（numeric modifier）
       clf — 类别修饰（classifier modifier） （文件，件）
      nmod — 复合名词修饰（noun compound modifier） （浦东，上海）
      amod — 形容词修饰（adjetive modifier） （情况，新）
    advmod — 副词修饰（adverbial modifier） （做到，基本）
      vmod — 动词修饰（verb modifier，participle modifier）
    prnmod — 插入词修饰（parenthetical modifier）
       neg — 不定修饰（negative modifier） (遇到，不)
       det — 限定词修饰（determiner modifier） （活动，这些）
     possm — 所属标记（possessive marker），NP
      poss — 所属修饰（possessive modifier），NP
      dvpm — DVP标记（dvp marker），DVP （简单，的）
    dvpmod — DVP修饰（dvp modifier），DVP （采取，简单）
      assm — 关联标记（associative marker），DNP （开发，的）
    assmod — 关联修饰（associative modifier），NP|QP （教训，特区）
      prep — 介词修饰（prepositional modifier） NP|VP|IP（采取，对）
     clmod — 从句修饰（clause modifier） （因为，开始）
     plmod — 介词性地点修饰（prepositional localizer modifier） （在，上）
       asp — 时态标词（aspect marker） （做到，了）
    partmod– 分词修饰（participial modifier） 不存在
       etc — 等关系（etc） （办法，等）
    中心语为实词
      conj — 联合(conjunct)
       cop — 系动(copula) 双指助动词？？？？
        cc — 连接(coordination)，指中心词与连词 （开发，与）
    其它
      attr — 属性关系 （是，工程）
    cordmod– 并列联合动词（coordinated verb compound） （颁布，实行）
      mmod — 情态动词（modal verb） （得到，能）
        ba — 把字关系
    tclaus — 时间从句 （以后，积累）
           — semantic dependent
       cpm — 补语化成分（complementizer），一般指“的”引导的CP （振兴，的）
     */
public class FlinkNLP {
    public static void main(String[] args) throws Exception {
        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // get input data
        long s1 = System.currentTimeMillis();
        String filePath = "/Users/sub/Desktop/Flink/考研词频实验/2013-2019英一/2013-2019英一.txt";
        //String filePath = "/Users/sub/Desktop/timemaster.txt";
        DataSet<String> text = env.readTextFile(filePath);
        long s2 = System.currentTimeMillis();
        DataSet<Tuple4<String,Integer,String,String>> result = text.flatMap(new WordNLP())
                .groupBy(0)
                .sum(1)
                .sortPartition(0, Order.ASCENDING)
                .setParallelism(1);
        //result.writeAsText("/Users/sub/Desktop/timemastersum.txt").setParallelism(1);
        result.print();
    }
    
    public final static class WordNLP implements FlatMapFunction<String, Tuple4<String, Integer, String, String>> {
        @Override
        public void flatMap(String value, Collector<Tuple4<String, Integer, String, String>> out) throws Exception {
            String[] tokens = value.toLowerCase().split("\\W+");
            int i,j,flag;
            Properties props = new Properties();  // set up pipeline properties
            props.put("annotators", "tokenize, ssplit, pos, lemma");   //分词、分句、词性标注和次元信息。
            StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
            for(i=1;i<tokens.length-1;i++){
                String txtWords; // 待处理文本
                flag=0;
                if(tokens[i].equals("times")){
                    String POS = "";
                    String POS1 = "";
                    String POS2 = "";
                    String P = "";
                    String Meaning = "";
                    txtWords = tokens[i-1]+" "+tokens[i];
                    Annotation document = new Annotation(txtWords);
                    pipeline.annotate(document);
                    List<CoreMap> words = document.get(CoreAnnotations.SentencesAnnotation.class);
                    for(CoreMap word_temp: words) {
                        j=0;
                        for (CoreLabel token: word_temp.get(CoreAnnotations.TokensAnnotation.class)) {
                            String word = token.get(CoreAnnotations.TextAnnotation.class);
                            String pos = token.get(CoreAnnotations.PartOfSpeechAnnotation.class);
                            if(j==0){
                                POS = POS + pos;
                                if(pos.equals("NNS")||pos.equals("NN")){
                                    POS1 ="名词";
                                    Meaning = "时代";
                                }
                                else if(pos.equals("IN")&&!word.equals("in")){
                                    POS1 ="介词";
                                    Meaning = "时代";
                                }
                                else if(pos.equals("DT")){
                                    POS1 ="限定词";
                                    Meaning = "时代";
                                }
                                else if(pos.equals("JJ") && !word.equals("many") && !word.equals("several")){
                                    POS1 ="形容词";
                                    Meaning = "时代";
                                }
                                else if(pos.equals("CD")){
                                    POS1 ="基数词";
                                    Meaning = "倍数/次数";
                                }
                                else if(word.equals("many") || word.equals("several")){
                                    POS1 ="形容词";
                                    Meaning = "倍数/次数";
                                }
                                else if(word.equals("in")){
                                    POS1 ="介词";
                                    Meaning = "短语"+ "\"" + "常常" + "\"";
                                }
                                j++;
                            }
                            else if(j==1){
                                POS = POS + "+" + pos;
                                POS2 = "名词";
                            }
                        }
                    }
                    P = "(" + POS1 + "+" + POS2 + ")";
                    POS = POS + P;
                    out.collect(new Tuple4<>(tokens[i-1]+" "+tokens[i], 1, POS, Meaning));
                }
                if(tokens[i].equals("time")){
                    String POS = "";
                    String POS1 = "";
                    String POS2 = "";
                    String P = "";
                    String Meaning = "";
                    txtWords = tokens[i-1]+" "+tokens[i];
                    Annotation document = new Annotation(txtWords);
                    pipeline.annotate(document);
                    List<CoreMap> words = document.get(CoreAnnotations.SentencesAnnotation.class);
                    for(CoreMap word_temp: words) {
                        j=0;
                        for (CoreLabel token: word_temp.get(CoreAnnotations.TokensAnnotation.class)) {
                            String word = token.get(CoreAnnotations.TextAnnotation.class);
                            String pos = token.get(CoreAnnotations.PartOfSpeechAnnotation.class);
                            if(j==0){
                                POS = POS + pos;
                                if(pos.equals("DT")){
                                    POS1 ="限定词";
                                    Meaning = "时间";
                                }
                                else if(pos.equals("NN")||pos.equals("NNS")||pos.equals("TO")||pos.equals("PRP")){
                                    flag=1;
                                }
                                else if(pos.equals("IN")&&!word.equals("in")&&!word.equals("on")&&!word.equals("a")){
                                    POS1 ="介词";
                                    Meaning = "时间";
                                }
                                else if(pos.equals("CC")){
                                    POS1 ="连词";
                                    Meaning = "时间";
                                }
                                else if(pos.equals("WDT")){
                                    POS1 ="疑问词";
                                    Meaning = "时间";
                                }
                                else if((pos.equals("JJ")|| pos.equals("JJR")||pos.equals("VBG"))
                                        &&!word.equals("first")&&!word.equals("second")&&!word.equals("third")
                                        &&!word.equals("last")){
                                    POS1 ="形容词";
                                    Meaning = "时间";
                                }
                                else if(word.equals("last")){
                                    POS1 = "形容词";
                                    Meaning = "次数";
                                }
                                else if(word.equals("first")||word.equals("second")||word.equals("third")){
                                    POS1 = "序数词";
                                    Meaning = "次数";
                                }
                                else if(pos.equals("CD")){
                                    POS1 ="基数词";
                                    Meaning = "倍数/次数";
                                }
                                else if(word.equals("in")){
                                    POS1 ="介词";
                                    Meaning = "短语"+ "\"" + "准时" + "\"";
                                }
                                else if(word.equals("on")){
                                    POS1 ="介词";
                                    Meaning = "短语"+ "\"" + "按时" + "\"";
                                }
                                else if(pos.equals("VB")||pos.equals("VBD")||pos.equals("VBN")||pos.equals("VBZ")){
                                    POS1 ="动词";
                                    Meaning = "时间";
                                }
                                else if(pos.equals("PRP$")){
                                    POS1 ="所有格";
                                    Meaning = "时间";
                                }
                                j++;
                            }
                            else if(j==1){
                                if(pos.equals("NN")) {
                                    POS = POS + "+" + pos;
                                    POS2 = "名词";
                                }
                                else if(pos.equals("VB")){
                                    POS = POS + "+" + pos;
                                    POS2 = "动词";
                                }
                            }
                        }
                    }
                    P = "(" + POS1 + "+" + POS2 + ")";
                    POS = POS + P;
                    if(flag==0){
                        out.collect(new Tuple4<>(tokens[i-1]+" "+tokens[i], 1, POS, Meaning));
                    }
                }
            }
        }
    }
}
