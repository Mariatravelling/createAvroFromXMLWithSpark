package utils; /**
 * Created by wangzehui on 11/9/15.
 */

import java.io.IOException;
import java.util.List;
import java.util.Properties;


import org.apache.pig.EvalFunc;

import org.apache.pig.data.*;

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.trees.EnglishGrammaticalStructure;
import edu.stanford.nlp.trees.GrammaticalStructure;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreeCoreAnnotations.TreeAnnotation;
import edu.stanford.nlp.util.CoreMap;

public class SentencesLenConstraint extends EvalFunc<DataBag> {
    TupleFactory mTupleFactory = TupleFactory.getInstance();
    BagFactory mBagFactory = BagFactory.getInstance();

    private static final int maxLen = 100;

    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        try {
            int userParser = Integer.parseInt(input.get(1).toString());
            if((userParser != 0) && (userParser != 1)){
                String msg = "use praser flag should be 0 or 1!";
                throw new RuntimeException(msg);
            }
            String text = input.get(0).toString();
            return getSentences(text, userParser);
        } catch (Exception e) {
            throw new IOException("Caught exception processing input row", e);
        }
    }

    private DataBag getSentences(String text, int userParser) {

        DataBag outerBag = mBagFactory.newDefaultBag();
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
        StanfordCoreNLP pipelineWithParser = new StanfordCoreNLP();

        Annotation document = new Annotation(text);
        pipeline.annotate(document);

        List<CoreMap> sentences = document.get(SentencesAnnotation.class);

        int sentenceCount = 0;
        for (CoreMap sentence : sentences) {

            int temp = userParser;
            int numberOfTokens = sentence.get(TokensAnnotation.class).size();
            Annotation sentAnnotation = new Annotation(sentence.toString());

            if ((userParser == 1) && (numberOfTokens <= maxLen)){
                pipelineWithParser.annotate(sentAnnotation);

            }else {
                pipeline.annotate(sentAnnotation);
                temp = 0;
                System.out.println("out of range sentence : " + sentence.toString());
            }



            //contains only one sentence
            List<CoreMap> fakeSentences = sentAnnotation.get(SentencesAnnotation.class);
            for (CoreMap fakeSentence : fakeSentences) {
                Tuple tuple = mTupleFactory.newTuple();
                tuple.append(sentenceCount);

                int startPosition = text.indexOf(fakeSentence.toString());

                Tuple s_spanTuple = mTupleFactory.newTuple();
                s_spanTuple.append(startPosition);
                s_spanTuple.append(startPosition + fakeSentence.toString().length());

                tuple.append(s_spanTuple);

                DataBag tokenBag = mBagFactory.newDefaultBag();
                for (CoreLabel token : fakeSentence.get(TokensAnnotation.class)) {
                    Tuple tokenTuple = mTupleFactory.newTuple();

                    // this is the POS tag of the token
                    String pos = token.get(PartOfSpeechAnnotation.class);
                    if(pos == null){
                        pos = "";
                    }
                    tokenTuple.append(pos);
                    // this is the NER label of the token
                    String ne = token.get(NamedEntityTagAnnotation.class);
                    if(ne == null){
                        ne = "";
                    }
                    tokenTuple.append(ne);

                    Tuple wordSpanTuple = mTupleFactory.newTuple();
                    wordSpanTuple.append(token.beginPosition());
                    wordSpanTuple.append(token.endPosition());

                    tokenTuple.append(wordSpanTuple);

                    tokenBag.add(tokenTuple);

                }
                tuple.append(tokenBag);


                if (temp > 0) {
                    // the parse tree of the current sentence
                    Tree tree = fakeSentence.get(TreeAnnotation.class);
                    tuple.append(tree.toString());


                    //Print ConLL format
                    EnglishGrammaticalStructure egs = new EnglishGrammaticalStructure(tree);
                    String str = GrammaticalStructure.dependenciesToString(egs, egs.typedDependencies(), tree, true, true);
                    tuple.append(str);

                } else {
                    tuple.append("");
                    tuple.append("");
                }
                outerBag.add(tuple);
                break;
            }
            sentenceCount++;

        }
        return outerBag;
    }
}