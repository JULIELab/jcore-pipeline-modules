# JCoRe Pipeline Modules

This is a multi module repository and project offering capabilities to create and run JCoRe UIMA pipelines. It has two main parts, the JCoRe Pipeline Builder(s) and the JCoRe Pipeline runner.

## Introduction

The [Unstructured Information Management Architecture (UIMA)](https://uima.apache.org/) is a component-based framework for the automated analysis of human-understandable artifacts like natural language or pictures with the goal to induce a computer-understandable structor onto them.
The [JULIE Lab  UIMA  Component  Repository (JCoRe)](https://github.com/JULIELab/jcore-base) is a collection of UIMA components for the analytics of natural language text developed at the JULIE Lab at the Friedrich Schiller Universität in Jena, Germany. This project is meant to facilitate the usage of the components without a deep understanding of UIMA or programming.
However, since UIMA as well as JCoRe are complex systems, some conventions and mechanics are required to successfully employ the tools offered here. The basic building blocks of UIMA should be known as they are described in the [UIMA Tutoral and Developer's Guides](https://uima.apache.org/d/uimaj-2.10.2/tutorials_and_users_guides.html#ugr.tug.aae.getting_started), chapter 1. Also, users should be aware that JCoRe is split into the [jcore-base](https://github.com/JULIELab/jcore-base) and [jcore-projects](https://github.com/JULIELab/jcore-projects) repositories. Both repositories are integrated into the JCoRe Pipeline Builders but there are conceptual differences users should be familiar with. For more information on the ideas and conventions behind JCoRe, please refer to the [jcore-base documentation](https://github.com/JULIELab/jcore-base).

### Installation

To build the project you require Maven >= 3 and Java JDK >= 11. In the root directory of the repository, execute

    mvn clean package
    
After a successful build, the following files are found in the subproject `target/` directories:

    1. jcore-pipeline-builder-cli/target/jcore-pipeline-builder-cli-*-jar-with-dependencies.jar
    2. jcore-pipeline-runner/jcore-pipeline-runner-base/target/jcore-pipeline-runner-base-*-cli-assembly.jar 
    3. jcore-pipeline-runner/jcore-pipeline-runner-cpe/target/jcore-pipeline-runner-cpe-*-jar-with-dependencies.jar
    
The first and second files are executable JARs. The third file must be co-located to the second for successfully running
a pipeline with the pipeline runner.

Thus, it is recommended to put all three files into one directory, e.g. `$(HOME)/bin`, and create aliases like these:

    alias editpipeline="java -jar $HOME/bin/jcore-pipeline-builder-*.jar"
    alias runpipeline="java -jar $HOME/bin/jcore-pipeline-runner-base-*.jar"
    
thus enabling the programs from anywhere with a simple command.

### Running the Pipeline Builder

To run the pipeline builder, follow the steps given in the *Installation* section and call `editpipeline` from the
command line. To edit an already existing pipeline, you can call `editpipeline <pipeline directory>` to open it
directly.

### Running the Pipeline Runner

The pipeline runner requires an XML configuration file. This file can be automatically created by calling
the pipeline runner and pointing it to a non-existing file like this:

    runpipeline run.xml
    
where `run.xml` does initially not exist. A configuration template will be written instead.
There will be a message similar to
 
    10:56:07.207 [main] INFO  o.a.c.b.FluentPropertyBeanIntrospector - Error when creating PropertyDescriptor for public final void org.apache.commons.configuration2.AbstractConfiguration.setProperty(java.lang.String,java.lang.Object)! Ignoring this property.
     
which can be ignored.

**NOTE: The above command did create a configuration file draft for you, you don't need to create it yourself but only to shorten it as is described below.**

We recommend to put the 
configuration file into the pipeline root folder. This file must be
edited with an editor of your choice before actually running a pipeline.

Opening the file, there are two `<runner>` elements. The first is named `CPERunner`, the second one
`DuccRunner`. One of those should be removed before running the pipeline as otherwise both runners will be called,
leading to errors with the `DuccRunner` which would require further configuration.

We will stick to the `CPERunner` for the remainder of this documentation. The `DuccRunner` section should be rather
self-explanatory for users familiar with [UIMA DUCC](https://uima.apache.org/doc-uimaducc-whatitam.html).

To remove the `DuccRunner` element, the 47 lines spanning the `DuccRunner` configuration (from its `<runner>` tag to the
closing `</runner>` tag) must be removed (tip: if using `vi`, just navigate to the opening tag, be sure to be in command
mode and type `d47<enter>` to delete those exact lines.)

The remaining configuration should look like this

    <?xml version="1.0" encoding="UTF-8" standalone="no"?>
    <configuration>
        <runners>
            <runner>
                <name>CPERunner</name>
                <pipelinepath>.</pipelinepath>
                <numthreads>1</numthreads>
                <heapsize>512M</heapsize>
            </runner>
        </runners>
    </configuration>
    
    
The `pipelinepath` element by default points to the current directory which makes it simple to run a pipeline
when the configuration XML file is put into the pipeline's root directory.

The `numthreads` element specifies the number of threads that should be employed for a run of the pipeline.
All components with the exception of the reader will be multiplied by the given number and be run in parallel. To avoid
reader bottlenecks, [`CAS Multipliers`](https://uima.apache.org/d/uimaj-2.10.4/tutorials_and_users_guides.html#ugr.tug.cm)
can be used. The `JCoRe` repositories offer `CAS Multipliers` for some reading components like the PubMed/Medline readers,
XML DB reader and the XMI DB reader.

The `heapsize` element set the maximum heap size for the JVM running the pipeline.

#### Running a Pipeline

To run the pipeline with the created configuration, issue the same command that was used to initially create
the configuration file itself, `runpipeline run.xml`. Now, that the file exists, it will be used to run the pipeline.
Internally, a class named `CPEBootstrapRunner` is called which reads the configuration and the pipeline data and
then starts a fresh process to include exactly the classpath items of the pipeline. 
