package org.warcbase.analysis.NER;

import java.util.List;

import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;

public class NEFinder {
  private String serializedClassifier = "classifiers/english.all.3class.distsim.crf.ser.gz";
  private AbstractSequenceClassifier<CoreLabel> classifier = CRFClassifier.getClassifierNoExceptions(serializedClassifier);
  
  public String replaceNER(String text){
    StringBuilder sb = new StringBuilder("");
    List<List<CoreLabel>> out = classifier.classify(text);
    Boolean prevNER = false;
    for (List<CoreLabel> sentence : out) {
      for (CoreLabel word : sentence) {
        if(word.get(CoreAnnotations.AnswerAnnotation.class).equals("O")){
          if(prevNER){
            prevNER = false;
            sb.append(' ');
          }
          sb.append(word.word() + ' ');
        }
        else{
          if(prevNER){
            //sb.append('_');
          }
          prevNER = true;
          sb.append(word.word());
        }
      }
    }
    return new String(sb);
  }
  
  public static void main(String[] args) {
    NEFinder nerFinder = new NEFinder();
    String s1 = "Good afternoon Rajat Raina, how are you today?";
    String s2 = "I go to school at Stanford University, which is located in California.";
    System.out.println(nerFinder.replaceNER(s1));
    System.out.println(nerFinder.replaceNER(s2));
  }
}
