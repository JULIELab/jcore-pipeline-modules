# jcore-pipeline-runner
A project to run UIMA pipelines in various manners with a focus on the JCoRe component repository.

## DUCC Pipeline Runner

This runner expects an existing UIMA DUCC (Distributed UIMA Cluster Computing) installation.
Its configuration file has the following form

    <configuration>
        <runners>
            <runner>
                <name>DuccRunner</name>
                <pipelinepath></pipelinepath>
                <configuration>
                    <jobfile></jobfile>
                    <jobdescription>
                        
                    </jobdescription>
                </configuration>
            </runner>
        </runners>
    </configuration>