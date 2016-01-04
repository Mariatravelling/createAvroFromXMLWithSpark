package utils;

import org.apache.pig.data.*;
import utils.SentencesLenConstraint;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Created by wangzehui on 11/9/15.
 */
public class testSentence {
    public static void main(String[] args)
    {
        TupleFactory mTupleFactory = TupleFactory.getInstance();
        BagFactory mBagFactory = BagFactory.getInstance();
        DataBag outerBag = mBagFactory.newDefaultBag();
        String text="Lawyers for three former preschool teachers said today that they would appeal a judge's ruling that tossed out part of a large civil suit alleging false arrest on charges of child abuse. He said the women had filed their suit too late.";
        Tuple tuple=mTupleFactory.newTuple(text);
        tuple.append(1);

        try {
            SentencesLenConstraint sentences=new SentencesLenConstraint();
            DataBag result = sentences.exec(tuple);
            int n=1;
            String content=" ";
            for ( Iterator<Tuple> iter = result.iterator();iter.hasNext();) {
                System.out.println("The " + n + " Sentence:");
                List str;
                str = iter.next().getAll();

                String t1 = str.get(1).toString();
                String[] offset = t1.split(",");
                String start_index = offset[0].substring(1);
                String end_index = offset[1].substring(0, offset[1].length() - 1);
                //System.out.println("start:"+start_index+" end:"+end_index);

                String t2 = str.get(2).toString();
                String[] tokens = t2.substring(2, t2.length() - 2).split("\\),\\(");
                String to = "";
                for (int i = 0; i < tokens.length; i++) {
                    String[] attributeOfTokens = tokens[i].split(",");
                    System.out.println("pos:" + attributeOfTokens[0]);
                    System.out.println("ner:" + attributeOfTokens[1]);
                    System.out.println("start_index:" + attributeOfTokens[2].substring(1));
                    System.out.println("end_index:" + attributeOfTokens[3].substring(0, attributeOfTokens[3].length() - 1));
                    to = to + attributeOfTokens[2].substring(1) + ";;" + attributeOfTokens[3].substring(0, attributeOfTokens[3].length() - 1) + ";;"
                            + attributeOfTokens[1] + ";;" + attributeOfTokens[0] + ";;";
                }

                content = content + start_index + ";;" + end_index + ";;" + to + ";;" + str.get(0).toString() + ";;" + str.get(3).toString() + ";;" + str.get(2).toString() + ";;";

                System.out.println(str.get(0)+"::"+str.get(1)+"::"+str.get(2)+"::"+str.get(3)+"::"+str.get(4));
                String sg=str.get(4).toString();
                String dp=str.get(3).toString();
                String s_id=str.get(0).toString();
                String span=str.get(1).toString();
                String token=str.get(2).toString();

                //System.out.println("Size:"+str.size());
                /*for(int j=0;j<str.size();j++)
                {
                    //System.out.println("Getting the "+j+" value:" + str.get(j));
                    content=content+str.get(j)+";;";
                }
                System.out.println("The content is:"+content);
                n++;
                */
            }
            System.out.println("COntent");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
