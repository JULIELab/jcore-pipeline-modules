package de.julielab.jcore.pipeline.builder.base.main;

import de.julielab.jcore.pipeline.builder.base.exceptions.PipelineIOException;
import org.apache.commons.io.FileUtils;
import org.apache.uima.analysis_engine.impl.AnalysisEngineDescription_impl;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.collection.impl.CollectionReaderDescription_impl;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;
import static org.junit.Assert.assertNotNull;

import java.io.File;

import static org.assertj.core.api.Assertions.*;

public class JCoReUIMAPipelineTest {
    @Test
    public void testLoadInvalid() throws PipelineIOException {
        assertThatExceptionOfType(PipelineIOException.class).
                isThrownBy(() -> new JCoReUIMAPipeline(new File("src/test/resources/testpipeline")).load(false))
                .withMessageContaining("There are multiple analysis engines");
    }


    @Test
    public void testSaveWithMultiplier() throws PipelineIOException {
        JCoReUIMAPipeline pipeline = new JCoReUIMAPipeline();
        Description crDesc = new Description();
        CollectionReaderDescription_impl cr = new CollectionReaderDescription_impl();
        cr.getMetaData().setName("The CR");
        crDesc.setDescriptor(cr);
        pipeline.setCrDescription(crDesc);

        Description cmDesc = new Description();
        AnalysisEngineDescription_impl cm = new AnalysisEngineDescription_impl();
        cm.getMetaData().setName("The CM");
        cm.getAnalysisEngineMetaData().getOperationalProperties().setOutputsNewCASes(true);
        cmDesc.setDescriptor(cm);
        pipeline.addCasMultiplier(cmDesc);

        File directory = new File("src/test/resources/pipelinestorage");
        FileUtils.deleteQuietly(directory);
        assertThatCode(() -> pipeline.store(directory)).doesNotThrowAnyException();
    }
}
