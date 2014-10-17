package edu.cmu.lti.f14.hw3.hw3_mtydykov.annotators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.uima.analysis_component.JCasAnnotator_ImplBase;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.FSIterator;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.FSList;
import org.apache.uima.jcas.tcas.Annotation;

import edu.cmu.lti.f14.hw3.hw3_mtydykov.typesystems.Document;
import edu.cmu.lti.f14.hw3.hw3_mtydykov.typesystems.Token;
import edu.cmu.lti.f14.hw3.hw3_mtydykov.utils.Utils;

/**
 * Create term vector for each document being processed.
 * @author mtydykov
 *
 */
public class DocumentVectorAnnotator extends JCasAnnotator_ImplBase {

  /**
   * Get document from current JCas and process it.
   */
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
   * A basic white-space tokenizer, it deliberately does not split on punctuation!
   * 
   * @param doc
   *          input text
   * @return a list of tokens.
   */
  List<String> tokenize0(String doc) {
    List<String> res = new ArrayList<String>();
    for (String s : doc.split("\\s+"))
      res.add(s);
    return res;
  }

  /**
   * Create term vector given document.
   * 
   * @param jcas
   * @param doc
   */

  private void createTermFreqVector(JCas jcas, Document doc) {
    String docText = doc.getText();
    List<String> tokens = tokenize0(docText);
    ArrayList<Token> tokenList = new ArrayList<Token>();
    HashMap<String, Integer> tokenToFreq = new HashMap<String, Integer>();
    // Count term frequencies.
    for (String token : tokens) {
      if (!tokenToFreq.containsKey(token)) {
        tokenToFreq.put(token, 1);
      } else {
        tokenToFreq.put(token, tokenToFreq.get(token) + 1);
      }
    }

    // Store terms and frequencies in Token objects.
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
