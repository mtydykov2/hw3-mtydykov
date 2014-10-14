package edu.cmu.lti.f14.hw3.hw3_mtydykov.annotators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.uima.analysis_component.JCasAnnotator_ImplBase;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.FSIterator;
import org.apache.uima.cas.Feature;
import org.apache.uima.cas.FeatureStructure;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.EmptyFSList;
import org.apache.uima.jcas.cas.FSList;
import org.apache.uima.jcas.cas.IntegerArray;
import org.apache.uima.jcas.cas.NonEmptyFSList;
import org.apache.uima.jcas.cas.StringArray;
import org.apache.uima.jcas.tcas.Annotation;

import edu.cmu.lti.f14.hw3.hw3_mtydykov.typesystems.Document;
import edu.cmu.lti.f14.hw3.hw3_mtydykov.typesystems.Token;
import edu.cmu.lti.f14.hw3.hw3_mtydykov.utils.Utils;

public class DocumentVectorAnnotator extends JCasAnnotator_ImplBase {

  @Override
  public void process(JCas jcas) throws AnalysisEngineProcessException {

    FSIterator<Annotation> iter = jcas.getAnnotationIndex().iterator();
    if (iter.isValid()) {
      iter.moveToNext();
      Document doc = (Document) iter.get();
      createTermFreqVector(jcas, doc);
    }

  }

  /**
   * 
   * @param jcas
   * @param doc
   */

  private void createTermFreqVector(JCas jcas, Document doc) {
    // TO DO: construct a vector of tokens and update the tokenList in CAS

    String docText = doc.getText();
    String[] tokens = docText.split("\\s");
    ArrayList<Token> tokenList = new ArrayList<Token>();
    HashMap<String, Integer> tokenToFreq = new HashMap<String, Integer>();
    for (String token : tokens) {
      if (!tokenToFreq.containsKey(token)) {
        tokenToFreq.put(token, 1);
      } else {
        tokenToFreq.put(token, tokenToFreq.get(token) + 1);
      }
    }
    for (String token : tokenToFreq.keySet()) {
      Token newTok = new Token(jcas);
      newTok.setText(token);
      newTok.setFrequency(tokenToFreq.get(token));
      tokenList.add(newTok);
    }
    FSList myList = Utils.fromCollectionToFSList(jcas, tokenList);
    doc.setTokenList(myList);

  }

}
