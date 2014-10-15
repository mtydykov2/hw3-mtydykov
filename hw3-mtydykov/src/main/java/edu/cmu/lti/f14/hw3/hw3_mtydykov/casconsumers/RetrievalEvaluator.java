package edu.cmu.lti.f14.hw3.hw3_mtydykov.casconsumers;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.uima.UIMAFramework;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.cas.FSIterator;
import org.apache.uima.cas.TypeSystem;
import org.apache.uima.cas.admin.CASFactory;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.collection.CasConsumer_ImplBase;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.FSList;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceProcessException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.util.InvalidXMLException;
import org.apache.uima.util.ProcessTrace;
import org.apache.uima.util.XMLInputSource;
import org.xml.sax.SAXException;

import edu.cmu.lti.f14.hw3.hw3_mtydykov.typesystems.Document;
import edu.cmu.lti.f14.hw3.hw3_mtydykov.typesystems.Token;
import edu.cmu.lti.f14.hw3.hw3_mtydykov.utils.Utils;

/**
 * Process all queries and documents and store necessary info.
 * Rank all processed documents according to cosine similarity with query
 * and output the highest-ranking relevant document per query along with 
 * its cosine similarity. Also output the MRR for the collection.
 * @author mtydykov
 *
 */
public class RetrievalEvaluator extends CasConsumer_ImplBase {
  private static final int QUERY_MARKER = 99;

  private static final String PARAM_OUTDIR = "outputFile";

  /** query id number **/
  public ArrayList<Integer> qIdList;

  /** query and text relevant values **/
  public ArrayList<Integer> relList;

  /** list of document texts **/
  public ArrayList<String> documentTexts;

  /** list of term vectors for each document **/
  public ArrayList<Map<String, Integer>> termVectors;

  /** map of query ID to index of document with the highest rank **/
  private HashMap<Integer, Integer> queryIdToHighestRank;

  /**
   * Initialize the necessary fields.
   */
  public void initialize() throws ResourceInitializationException {
    qIdList = new ArrayList<Integer>();
    relList = new ArrayList<Integer>();
    documentTexts = new ArrayList<String>();
    termVectors = new ArrayList<Map<String, Integer>>();
    queryIdToHighestRank = new HashMap<Integer, Integer>();
  }

  /**
   * Add information from the given CAS to fields.
   */
  @Override
  public void processCas(CAS aCas) throws ResourceProcessException {

    JCas jcas;
    try {
      jcas = aCas.getJCas();
    } catch (CASException e) {
      throw new ResourceProcessException(e);
    }

    FSIterator it = jcas.getAnnotationIndex(Document.type).iterator();

    if (it.hasNext()) {
      Document doc = (Document) it.next();

      // Make sure that your previous annotators have populated this in CAS
      FSList fsTokenList = doc.getTokenList();
      ArrayList<Token> tokens = Utils.fromFSListToCollection(fsTokenList, Token.class);
      qIdList.add(doc.getQueryID());
      relList.add(doc.getRelevanceValue());
      Map<String, Integer> tokenToFreqMap = convertTokenArrayListToMap(tokens);
      termVectors.add(tokenToFreqMap);
      documentTexts.add(doc.getText());
    }

  }

  /**
   * Convert list of tokens to a map of token to frequency.
   * @param tokens
   * @return
   */
  public static Map<String, Integer> convertTokenArrayListToMap(ArrayList<Token> tokens) {
    Map<String, Integer> tokenToFreqMap = new HashMap<String, Integer>();
    for (Token token : tokens) {
      tokenToFreqMap.put(token.getText(), token.getFrequency());
    }
    return tokenToFreqMap;
  }

  /**
   * Once collection is done processing, compute cosine similarity and MRR.
   */
  @Override
  public void collectionProcessComplete(ProcessTrace arg0) throws ResourceProcessException,
          IOException {
    // get ready to write output
    String outputFileName = (String) getConfigParameterValue(PARAM_OUTDIR);
    File output = new File(outputFileName);
    PrintWriter outputWriter = new PrintWriter(output);
    super.collectionProcessComplete(arg0);
    
    // populate map of the query ID to the index in the list that holds the query itself
    HashMap<Integer, Integer> queryIdToIndex = new HashMap<Integer, Integer>();
    for (int j = 0; j < relList.size(); j++) {
      if (relList.get(j) == QUERY_MARKER) {
        queryIdToIndex.put(qIdList.get(j), j);
      }
    }
    
    // populate map of query ID to list of indeces holding the relevant documents (other than the query)
    HashMap<Integer, ArrayList<Integer>> queryIdToDocs = new HashMap<Integer, ArrayList<Integer>>();
    for (int j = 0; j < relList.size(); j++) {
      if (relList.get(j) != QUERY_MARKER) {
        if (!queryIdToDocs.containsKey(qIdList.get(j))) {
          queryIdToDocs.put(qIdList.get(j), new ArrayList<Integer>());
        }
        queryIdToDocs.get(qIdList.get(j)).add(j);
      }
    }

    HashSet<Integer> queryIds = new HashSet<Integer>(qIdList);

    computeCosineSimilarityForAllQueries(outputWriter, queryIdToIndex, queryIdToDocs, queryIds);

    // compute MRR over the entire set of queries
    double metric_mrr = compute_mrr();
    System.out.println(" (MRR) Mean Reciprocal Rank ::" + metric_mrr);
    outputWriter.write("MRR=" + metric_mrr);
    outputWriter.close();
  }

  /**
   * Compute cosine similarities for all queries & documents that were processed.
   * @param outputWriter
   * @param queryIdToIndex
   * @param queryIdToDocs
   * @param queryIds
   */
  private void computeCosineSimilarityForAllQueries(PrintWriter outputWriter,
          HashMap<Integer, Integer> queryIdToIndex,
          HashMap<Integer, ArrayList<Integer>> queryIdToDocs, HashSet<Integer> queryIds) {
    // compute the cosine similarity measure
    for (int queryId : queryIds) {
      // get the term vector for the query
      Map<String, Integer> queryMap = termVectors.get(queryIdToIndex.get(queryId));
      HashMap<Integer, Double> docToCosineSim = new HashMap<Integer, Double>();
      
      // compute cosine similarity for each document for the given query ID
      for (int docIndex : queryIdToDocs.get(queryId)) {
        Map<String, Integer> termMap = termVectors.get(docIndex);
        double currCosineSimilarity = computeCosineSimilarity(queryMap, termMap);
        docToCosineSim.put(docIndex, currCosineSimilarity);
      }

      List<Entry<Integer, Double>> list = sortDocIndecesByCosineSimilarity(docToCosineSim);
      
      int rankIndex = getHighestRankIndexForQuery(list);
      
      // rankIndex is now index of highest-ranked relevant doc;
      // use it to get the index of the document itself
      int highestRelDocIndex = list.get(rankIndex).getKey();
      double highestRolDocCosine = list.get(rankIndex).getValue();
      
      // the actual rank is 1 higher than the rankIndex
      int rank = rankIndex + 1;
      
      // store the query along with the rank of the highest relevant doc
      queryIdToHighestRank.put(queryId, rank);
      
      // write output to file
      outputWriter.write("cosine=" + highestRolDocCosine + "\trank=" + rank + "\tqid=" + queryId
              + "\trel=" + relList.get(highestRelDocIndex) + "\t"
              + documentTexts.get(highestRelDocIndex) + "\n");
    }
  }

  /**
   * Given a ranked list of pairs of document indeces and cosine similarities
   * for a query, return the rank index of the highest-ranked relevant document.
   * @param list
   * @return
   */
  private int getHighestRankIndexForQuery(List<Entry<Integer, Double>> list) {
    // look for first relevant document @ highest rank
    boolean relFound = false;
    int rankIndex = 0;
    while (rankIndex < list.size() && !relFound) {
      // if relevant doc found
      if (relList.get(list.get(rankIndex).getKey()) == 1) {
        relFound = true;
      } else {
        rankIndex++;
      }
    }
    
    // no relevant documents - just use whatever item got ranked as first
    if (!relFound) {
      rankIndex = 0;
    }
    return rankIndex;
  }

  /**
   * Given a map of document index to cosine similarity with its 
   * query, return a ranked list of pairs.
   * @param docToCosineSim
   * @return
   */
  private List<Entry<Integer, Double>> sortDocIndecesByCosineSimilarity(
          HashMap<Integer, Double> docToCosineSim) {
    // sort document indeces by cosine similarity
    Set<Entry<Integer, Double>> set = docToCosineSim.entrySet();
    List<Entry<Integer, Double>> list = new ArrayList<Entry<Integer, Double>>(set);
    Collections.sort(list, new Comparator<Map.Entry<Integer, Double>>() {
      public int compare(Map.Entry<Integer, Double> o1, Map.Entry<Integer, Double> o2) {
        // same cosine similarity - rank by relevance 
        // (which is guaranteed to be 1 or 0, since only non-query
        // documents are in the list at this point)
        if(o2.getValue() == o1.getValue()){
          if(relList.get(o2.getKey()) > relList.get(o1.getKey())){
            return 1;
          } else if(relList.get(o2.getKey()) > relList.get(o1.getKey())){
            return -1;
          }
          return 0;
        }
        return (o2.getValue()).compareTo(o1.getValue());
      }
    });
    return list;
  }

  /**
   * Calculate cosine similarity, given query and doc vectors
   * @return cosine_similarity
   */
  private double computeCosineSimilarity(Map<String, Integer> queryVector,
          Map<String, Integer> docVector) {

    // get the set of all words used in query & doc
    HashSet<String> globalWords = new HashSet<String>();
    globalWords.addAll(queryVector.keySet());
    globalWords.addAll(docVector.keySet());
    
    double numerator = calculateNumerator(queryVector, docVector, globalWords);

    double denominator = calculateDenominator(queryVector, docVector);

    return numerator / denominator;
  }

  /**
   * Calculate denominator for cosine similarity.
   * @param queryVector
   * @param docVector
   * @return
   */
  private double calculateDenominator(Map<String, Integer> queryVector,
          Map<String, Integer> docVector) {
    return Math.sqrt(getMagnitude(queryVector) * getMagnitude(docVector));
  }

  /**
   * Calculate numerator for cosine similarity.
   * @param queryVector
   * @param docVector
   * @param globalWords
   * @return
   */
  private double calculateNumerator(Map<String, Integer> queryVector,
          Map<String, Integer> docVector, HashSet<String> globalWords) {
    double numerator = 0.0;
    // calculate numerator 
    for (String term : globalWords) {
      int queryFreq = 0;
      if (queryVector.containsKey(term)) {
        queryFreq = queryVector.get(term);
      }
      int docFreq = 0;
      if (docVector.containsKey(term)) {
        docFreq = docVector.get(term);
      }
      numerator += queryFreq * docFreq;
    }
    return numerator;
  }

  /**
   * Get magnitude given a term vector.
   * @param vals
   * @return
   */
  public static double getMagnitude(Map<String, Integer> vals) {
    double magnitude = 0;
    for (String term : vals.keySet()) {
      magnitude += Math.pow(vals.get(term), 2);
    }

    return magnitude;

  }

  /**
   * Calculate MRR.
   * @return mrr
   */
  private double compute_mrr() {
    double metric_mrr = 0.0;
    for (int queryId : queryIdToHighestRank.keySet()) {
      metric_mrr += (double) 1 / queryIdToHighestRank.get(queryId);
    }
    metric_mrr = metric_mrr / (queryIdToHighestRank.size());
    return metric_mrr;
  }

}
