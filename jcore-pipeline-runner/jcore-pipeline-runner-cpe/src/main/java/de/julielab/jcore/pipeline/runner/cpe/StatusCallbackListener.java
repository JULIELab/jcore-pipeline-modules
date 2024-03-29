package de.julielab.jcore.pipeline.runner.cpe;

import de.julielab.jcore.types.Header;
import de.julielab.jcore.types.casmultiplier.JCoReURI;
import de.julielab.jcore.types.casmultiplier.RowBatch;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.cas.FSIterator;
import org.apache.uima.collection.CollectionProcessingEngine;
import org.apache.uima.collection.EntityProcessStatus;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class StatusCallbackListener implements org.apache.uima.collection.StatusCallbackListener {

    private final static Logger LOGGER = LoggerFactory.getLogger(StatusCallbackListener.class);
    int entityCount = 0;
    private final CollectionProcessingEngine cpe;
    /**
     * list holding primary keys of documents that have been successfully
     * processed
     */
    private final ArrayList<byte[][]> processed = new ArrayList<>();
    /**
     * list holding primary keys of documents during the processing of which an
     * exception occured
     */
    private final ArrayList<byte[][]> exceptions = new ArrayList<>();
    /**
     * matches primary keys of unsuccessfully processed documents and exceptions
     * that occured during the processing
     */
    private final HashMap<byte[][], String> logException = new HashMap<>();
    /**
     * Start time of the processing
     */
    private long mInitCompleteTime;

    public StatusCallbackListener(CollectionProcessingEngine cpe, Integer batchSize) {
        this.cpe = cpe;
    }

    /**
     * Called when the initialization is completed.
     *
     * @see org.apache.uima.collection.processing.StatusCallbackListener#initializationComplete()
     */
    public void initializationComplete() {
        LOGGER.info("CPE Initialization complete");
        mInitCompleteTime = System.currentTimeMillis();
        long mBatchTime = System.currentTimeMillis();
    }

    /**
     * Called when the batchProcessing is completed.
     *
     * @see org.apache.uima.collection.processing.StatusCallbackListener#batchProcessComplete()
     */
    public synchronized void batchProcessComplete() {
        processed.clear();
        LOGGER.info("Completed " + entityCount + " documents");
    }

    /**
     * Called when the collection processing is completed. Exits the application
     * in case it doesn't exit on itself (happens e.g. with JREX because of the
     * ExecutorService; we have problems to shut all of them down).
     *
     * @see org.apache.uima.collection.processing.StatusCallbackListener#collectionProcessComplete()
     */
    public synchronized void collectionProcessComplete() {

        long time = System.currentTimeMillis();
        LOGGER.info("Completed " + entityCount + " documents");
        long processingTime = time - mInitCompleteTime;
        LOGGER.info("Processing Time: " + processingTime + " ms");
        LOGGER.info("\n\n ------------------ PERFORMANCE REPORT ------------------\n");
        LOGGER.info(cpe.getPerformanceReport().toString());
        System.exit(0);
    }

    /**
     * Called when the CPM is paused.
     *
     * @see org.apache.uima.collection.processing.StatusCallbackListener#paused()
     */
    public void paused() {
        LOGGER.info("Paused");
    }

    /**
     * Called when the CPM is resumed after a pause.
     *
     * @see org.apache.uima.collection.processing.StatusCallbackListener#resumed()
     */
    public void resumed() {
        LOGGER.info("Resumed");
    }

    /**
     * Called when the CPM is stopped abruptly due to errors.
     *
     * @see org.apache.uima.collection.processing.StatusCallbackListener#aborted()
     */
    public void aborted() {
        LOGGER.info("The CPE has been aborted by the framework. The JVM is forcibly quit to avoid the application getting stuck on some threads that could not be stopped.");
        System.exit(1);
    }

    /**
     * Called when the processing of a document is completed. <br>
     * The process status can be looked at and corresponding actions taken.
     *
     * @param aCas    CAS corresponding to the completed processing
     * @param aStatus EntityProcessStatus that holds the status of all the events
     *                for an entity
     */
    public synchronized void entityProcessComplete(CAS aCas, EntityProcessStatus aStatus) {
        try {
            JCas jCas = aCas.getJCas();
            FSIterator<JCoReURI> multiplierUris = jCas.getTypeSystem().getType(JCoReURI.class.getCanonicalName()) != null ? jCas.<JCoReURI>getAnnotationIndex(JCoReURI.type).iterator() : null;
            FSIterator<RowBatch> dbMultiplierBatch = jCas.getTypeSystem().getType(RowBatch.class.getCanonicalName()) != null ? jCas.<RowBatch>getAnnotationIndex(RowBatch.type).iterator() : null;
            List<JCoReURI> multiplierUriList = multiplierUris != null && multiplierUris.hasNext() ? new ArrayList<>() : Collections.emptyList();
            List<RowBatch> rowBatchList = dbMultiplierBatch != null && dbMultiplierBatch.hasNext()? new ArrayList<>() : Collections.emptyList();
            if (multiplierUris != null && multiplierUris.hasNext()) {
                while (multiplierUris.hasNext()) {
                    multiplierUriList.add(multiplierUris.next());
                    ++entityCount;
                }
            } else if (dbMultiplierBatch != null && dbMultiplierBatch.hasNext()) {
                while (dbMultiplierBatch.hasNext()) {
                    rowBatchList.add(dbMultiplierBatch.next());
                    ++entityCount;
                }
            } else {
                ++entityCount;
            }
            String docId = "<unknown>";
            try {
                final Header header = JCasUtil.selectSingle(jCas, Header.class);
                docId = header.getDocId();
            } catch (IllegalArgumentException e) {
                LOGGER.debug("Document occurred that did not have Header annotation.");
            }
            if (!aStatus.isException()) {
                LOGGER.debug("Document with ID {} finished processing.", docId);
            } else {
                String filename = "pipeline-error-" + docId + "-"+new Date()+".err";
                LOGGER.error("Exception occurred while processing document with ID {}: {} Writing error message to {}", docId, aStatus.getExceptions(), filename);
                LOGGER.error("Components failed: {}", aStatus.getFailedComponentNames());
                LOGGER.error("Error message: {}", aStatus.getStatusMessage());
                LOGGER.error("Process trace: {}", aStatus.getProcessTrace().toString());
                LOGGER.error("Last exception: ", aStatus.getExceptions().get(0));
//                if (multiplierUriList != null)
//                    LOGGER.error("This CAS was created for a multiplier. The following URIs are contained: {}", multiplierUriList.stream().map(JCoReURI::getUri).collect(Collectors.toList()));
                final String log = createLog(aStatus, multiplierUriList, rowBatchList);
                try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename), StandardCharsets.UTF_8))) {
                    bw.write(log);
                    bw.newLine();
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (CASException | IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * Create log entry for an exception that occured during the processing of a
     * document
     *  @param status the status that is an exception
     * @param multiplierUriList
     * @param rowBatchList
     */
    public String createLog(EntityProcessStatus status, List<JCoReURI> multiplierUriList, List<RowBatch> rowBatchList) {
        String ls = System.getProperty("line.separator");
        StringBuilder builder = new StringBuilder();

        builder.append("Error happened on: ").append(new Date());
        builder.append(ls);
        builder.append("-------------- Failed Components -------------- ").append(ls);
        List<String> componentNames = status.getFailedComponentNames();
        for (int i = 0; i < componentNames.size(); i++) {
            builder.append(i + 1).append(". ").append(componentNames.get(i)).append(ls);
        }

        builder.append("-------------- Stack Traces -------------- ").append(ls);
        List<Exception> exceptions = status.getExceptions();
        for (Object exception : exceptions) {
            Throwable throwable = (Throwable) exception;
            appendThrowableLog(builder, throwable);

            while (throwable.getCause() != null) {
                throwable = throwable.getCause();
                appendThrowableLog(builder, throwable);
            }
        }

        if (!multiplierUriList.isEmpty()) {
            builder.append(ls);
            builder.append("This CAS was meant for a multiplier. URIs meant for the multiplier contained in this CAS:");
            for (JCoReURI jCoReURI : multiplierUriList) {
                builder.append(jCoReURI.getUri()).append(ls);
            }
        }
        if (!rowBatchList.isEmpty()) {
            builder.append(ls);
            builder.append("This CAS was meant for a database multiplier. Primary keys meant for the multiplier contained in this CAS:");
            for (RowBatch row : rowBatchList) {
                if (row.getIdentifiers() != null) {
                    builder.append(String.join(",", row.getIdentifiers().toStringArray()));
                }
                builder.append(ls);
            }
        }

        return builder.toString();
    }

    private void appendThrowableLog(StringBuilder builder, Throwable throwable) {
        StringWriter writer = new StringWriter();
        throwable.printStackTrace(new PrintWriter(writer));
        builder.append(writer);
    }

    public CollectionProcessingEngine getCpe() {
        return cpe;
    }
}
