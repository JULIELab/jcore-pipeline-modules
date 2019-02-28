package de.julielab.jcore.pipeline.builder.base.main;

import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.collection.impl.metadata.cpe.*;
import org.apache.uima.collection.metadata.*;
import org.apache.uima.resource.metadata.impl.Import_impl;

public class CPE {
    private CpeDescription cpe;

    public CPE() {
        cpe = new CpeDescriptionImpl();
    }

    public void setCollectionReader(Description reader) throws CpeDescriptorException {
        final CpeCollectionReader cpeReader = new CpeCollectionReaderImpl();
        final CpeCollectionReaderIteratorImpl readerIterator = new CpeCollectionReaderIteratorImpl();
        readerIterator.setDescriptor(createCpeComponentDescriptor(reader));
        cpeReader.setCollectionIterator(readerIterator);
        cpe.setAllCollectionCollectionReaders(new CpeCollectionReader[]{cpeReader});
    }

    public void setAnalysisEngine(Description engine) throws CpeDescriptorException {
        CpeCasProcessors cpeCasProcessors = cpe.getCpeCasProcessors();
        if (cpeCasProcessors == null) {
            cpeCasProcessors = new CpeCasProcessorsImpl();
            cpeCasProcessors.setPoolSize(24);
            cpe.setCpeCasProcessors(cpeCasProcessors);
        }
        CpeIntegratedCasProcessorImpl processor = new CpeIntegratedCasProcessorImpl();
        processor.setCpeComponentDescriptor(createCpeComponentDescriptor(engine));
        cpeCasProcessors.addCpeCasProcessor(processor);
    }

    private CpeComponentDescriptor createCpeComponentDescriptor(Description componentDescription) {
        return createCpeComponentDescriptor(componentDescription.getUimaDescPath());
    }

    private CpeComponentDescriptor createCpeComponentDescriptor(String location) {
        final CpeComponentDescriptor componentDescriptor = new CpeComponentDescriptorImpl();
        final Import_impl imp = new Import_impl();
        imp.setLocation(location);
        componentDescriptor.setImport(imp);
        return componentDescriptor;
    }

    public CpeDescription getDescription() {
        return cpe;
    }

    public void setAnalysisEngine(String descriptorLocation, String casProcessorName) throws CpeDescriptorException {
        CpeCasProcessors cpeCasProcessors = cpe.getCpeCasProcessors();
        if (cpeCasProcessors == null) {
            cpeCasProcessors = new CpeCasProcessorsImpl();
            cpe.setCpeCasProcessors(cpeCasProcessors);
        }
        CpeIntegratedCasProcessorImpl processor = new CpeIntegratedCasProcessorImpl();
        processor.setCpeComponentDescriptor(createCpeComponentDescriptor(descriptorLocation));
        processor.setName(casProcessorName);
        processor.setBatchSize(500);
        cpeCasProcessors.addCpeCasProcessor(processor);
    }
}
