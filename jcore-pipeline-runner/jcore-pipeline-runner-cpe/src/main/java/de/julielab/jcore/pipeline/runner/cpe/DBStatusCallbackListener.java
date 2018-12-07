/**
 * DBStatusCallbackListener.java
 * <p>
 * Copyright (c) 2008, JULIE Lab.
 * All rights reserved. This program and the accompanying materials
 * are protected. Please contact JULIE Lab for further information.
 * <p>
 * Author: landefeld
 * <p>
 * Current version: //TODO insert current version number
 * Since version:   //TODO insert version number of first appearance of this class
 * <p>
 * Creation date: 16.09.2008
 * <p>
 * //TODO insert short description
 **/

package de.julielab.jcore.pipeline.runner.cpe;

import de.julielab.jcore.types.Header;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.collection.CollectionProcessingEngine;
import org.apache.uima.collection.EntityProcessStatus;
import org.apache.uima.collection.StatusCallbackListener;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class DBStatusCallbackListener implements StatusCallbackListener {

    private final static Logger LOGGER = LoggerFactory.getLogger(DBStatusCallbackListener.class);
    int entityCount = 0;
    private CollectionProcessingEngine cpe;
    /**
     * list holding primary keys of documents that have been successfully
     * processed
     */
    private ArrayList<byte[][]> processed = new ArrayList<byte[][]>();
    /**
     * list holding primary keys of documents during the processing of which an
     * exception occured
     */
    private ArrayList<byte[][]> exceptions = new ArrayList<byte[][]>();
    /**
     * matches primary keys of unsuccessfully processed documents and exceptions
     * that occured during the processing
     */
    private HashMap<byte[][], String> logException = new HashMap<byte[][], String>();
    /**
     * Start time of the processing
     */
    private long mInitCompleteTime;
    private long mBatchTime;
    private Integer batchSize;

    public DBStatusCallbackListener(CollectionProcessingEngine cpe, Integer batchSize) {
        this.cpe = cpe;
        this.batchSize = batchSize;
    }

    /**
     * Called when the initialization is completed.
     *
     * @see org.apache.uima.collection.processing.StatusCallbackListener#initializationComplete()
     */
    public void initializationComplete() {
        LOGGER.info("CPE Initialization complete");
        mInitCompleteTime = System.currentTimeMillis();
        mBatchTime = System.currentTimeMillis();
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
        LOGGER.info("The CPE has been aborted by the framework. The JVM ist forcibly quit to avoid the application getting stuck on some threads that could not be stopped.");
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
            final Header header = JCasUtil.selectSingle(jCas, Header.class);
            if (!aStatus.isException()) {
                LOGGER.debug("Document with ID {} finished processing.", header.getId());
            } else {
                String filename = "pipeline-error-" + header.getId();
                LOGGER.debug("Exception occurred while processing document with ID {}. Writing error message to {}", header.getId(), aStatus.getExceptions(), filename);
                final String log = createLog(aStatus);
                try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(log), StandardCharsets.UTF_8))) {
                    bw.write(log);
                    bw.newLine();
                }
            }
        } catch (CASException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        entityCount++;

    }

    /**
     * Create log entry for an exception that occured during the processing of a
     * document
     *
     * @param status the status that is an exception
     */
    public String createLog(EntityProcessStatus status) {
        StringBuilder builder = new StringBuilder();

        builder.append("Error happened on: " + new Date());
        builder.append("-------------- Failed Components -------------- \n");
        @SuppressWarnings("rawtypes")
        List componentNames = status.getFailedComponentNames();
        for (int i = 0; i < componentNames.size(); i++) {
            builder.append((i + 1) + ". " + componentNames.get(i) + "\n");
        }

        builder.append("-------------- Stack Traces -------------- \n");
        @SuppressWarnings("rawtypes")
        List exceptions = status.getExceptions();
        for (int i = 0; i < exceptions.size(); i++) {
            Throwable throwable = (Throwable) exceptions.get(i);
            StringWriter writer = new StringWriter();
            throwable.printStackTrace(new PrintWriter(writer));
            builder.append(writer.toString());
        }

        return builder.toString();
    }

    public CollectionProcessingEngine getCpe() {
        return cpe;
    }
}
