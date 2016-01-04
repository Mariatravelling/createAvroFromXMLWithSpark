package utils;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by wangzehui on 11/13/15.
 */
public class parseSentence {

    static DataBag result;
    public parseSentence(String text)
    {
        TupleFactory mTupleFactory = TupleFactory.getInstance();
        BagFactory mBagFactory = BagFactory.getInstance();
        DataBag outerBag = mBagFactory.newDefaultBag();
        Tuple tuple = mTupleFactory.newTuple(text);
        tuple.append(1);
        try {
        SentencesLenConstraint sentences = new SentencesLenConstraint();
            result = sentences.exec(tuple);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public List<String> gettingSentences () {

        String content ="";
            List parseDetails=new ArrayList();
            int n = 1;
            for (Iterator<Tuple> iter = result.iterator(); iter.hasNext(); ) {
                List str = iter.next().getAll();

                String t1=str.get(1).toString();
                String span=parseSentence.gettingSpan(t1);
                parseDetails.add(0,span);

                String t2=str.get(2).toString();
                String tokens=parseSentence.gettingtokens(t2);
                parseDetails.add(1,tokens);
                System.out.println("The " + n + " Sentence:");
                System.out.println("Size:" + str.size());
                for (int j = 0; j < str.size(); j++) {
                    content=content+str.get(j)+";;";
                }
                parseDetails.add(2,content);

                String sg=str.get(4).toString();
                String dp=str.get(3).toString();
                String s_id=str.get(0).toString();
                String span1=str.get(1).toString();
                String token=str.get(2).toString();
                n++;
            }

        return parseDetails;
    }

    public int countSentence()
    {
        int count=(int)result.size();
        return count;
    }

    public static String gettingSpan(String span)
    {
        String[] offset=span.split(",");
        String start_index=offset[0].substring(1);
        String end_index=offset[1].substring(0,offset[1].length()-1);
        String parsingSpan=start_index+";;"+end_index;
        return parsingSpan;
    }

    public static String gettingtokens(String tokens)
    {

        String[] token=tokens.substring(2,tokens.length()-2).split("\\),\\(");
        String parsingTokens="";
        for(int i=0;i<token.length;i++) {
            String[] attributeOfTokens = token[i].split(",");
            String pos=attributeOfTokens[0];
            String ner=attributeOfTokens[1];
            String start_index=attributeOfTokens[2].substring(1);
            String end_index=attributeOfTokens[3].substring(0,attributeOfTokens[3].length()-1);
            //System.out.println("pos:" + attributeOfTokens[0]);
            //System.out.println("ner:" + attributeOfTokens[1]);
            //System.out.println("start_index:" + attributeOfTokens[2].substring(1));
            //System.out.println("end_index:" + attributeOfTokens[3].substring(0,attributeOfTokens[3].length()-1));
            parsingTokens=parsingTokens+pos+";;"+ner+";;"+start_index+";;"+end_index+";;";
        }
        return parsingTokens;
    }
}
