package utils;

import edu.stanford.nlp.dcoref.CorefChain;
import edu.stanford.nlp.dcoref.CorefCoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

import java.util.List;
import java.util.Properties;


/**
 * Created by wangzehui on 11/12/15.
 */
public class HelloNLP {
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        // creates a StanfordCoreNLP object, with POS tagging, lemmatization, NER, parsing, and coreference resolution
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

        // read some text in the text variable

        String text = "SAP is the global market leader for business software and thus contributes a considerable part of the world's economic power grid. At SAP you get your chance to put your ideas into action with maximum impact." ;// Add your text here!

        // create an empty Annotation just with the given text
        Annotation document;
        document = new Annotation(text);

        // run all Annotators on this text
        pipeline.annotate(document);

        // these are all the sentences in this document
        // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
        List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);
        int n=1;
        for(CoreMap sentence: sentences) {

            System.out.println("the"+n+":"+sentence);
            n++;
            // traversing the words in the current sentence
            // a CoreLabel is a CoreMap with additional token-specific methods
	      /*for (CoreLabel token: sentence.get(TokensAnnotation.class)) {
	        // this is the text of the token
	        String word = token.get(TextAnnotation.class);
	        // this is the POS tag of the token
	        String pos = token.get(PartOfSpeechAnnotation.class);
	        // this is the NER label of the token
	        String ne = token.get(NamedEntityTagAnnotation.class);
	        System.out.println("NE:"+ne);
	        String dcoref=token.get(SpeakerAnnotation.class);
	        System.out.println("dcoref"+dcoref);
	      }*/

            // this is the parse tree of the current sentence
            //Tree tree = sentence.get(TreeAnnotation.class);

            // this is the Stanford dependency graph of the current sentence
            //SemanticGraph dependencies = sentence.get(CollapsedCCProcessedDependenciesAnnotation.class);
        }

        // This is the coreference link graph
        // Each chain stores a set of mentions that link to each other,
        // along with a method for getting the most representative mention
        // Both sentence and token offsets start at 1!
        /*Map<Integer, CorefChain> graph =  document.get(CorefCoreAnnotations.CorefChainAnnotation.class);
        for (Map.Entry<Integer, CorefChain> entry : graph.entrySet()) {
            String key = entry.getKey().toString();;
            CorefChain value = entry.getValue();
            System.out.println("key, " + key + " value " + value );
        }*/
    }
}
