# Changelog

## v0.2.5.1 (23/05/2019)
This release contains a backport to allow the heap size specification for the actual pipeline run.

There is the `CPEBootstrapRunner` which loads the `JCoReUimaPipeline` and then creates a child process that actually runs the pipeline. The `heapsize` configuration property in the pipeline runner XML configuration file allows the heap size specification for the child process which actually runs the pipeline.
---

## v0.3.1 (29/03/2019)
The last version did not allow to leave the update components dialog any more.
This has been fixed.
---

## v0.3.0 (28/03/2019)
This is a major release that breaks compatibility with formerly created pipelines.

Major fixes and enhancements:
* The serialization format for the component meta data has been changed from Java binary serialization to a JSON serialization.
* Fixed the issue that multiple components with the exact same name could be present in one pipeline. This lead to a diverse range of issues. Now, multiple components of the same type are enumerated instead of sharing the same name.
* PEARs are now wrapped into an AAE for which a descriptor is stored in the `descAll/` and `desc/` directories with all other descriptors.

---

## v0.2.8 (02/01/2019)
This release allows to deactivate components. When saving the pipeline from the CLI, the deactivated components will not be written into the `desc/` directory and thus they won't be used when running the pipeline.
Their data is still stored in the `*.bin` files of the pipeline and can be reactivated using the pipeline builder CLI.
---

## v.0.2.6 (12/12/2018)
In this release, all descriptors use their original type system imports. Previous releases created descriptors that included all types right within the descriptors. This has issues when the type system changes over time but the descriptor stored in the pipeline does not update.
---

## v0.2.5 (09/12/2018)
Various enhancements and bug fixes.
The most notable change is that the CPE runner is now a project of its own. The original `CPERunner` class, residing now in `jcore-pipeline-runner-base`, has been renamed to `CPEBootstrapRunner`. Instead of loading the classpath of the pipeline to run into the runner's JVM, a new process is started via `Runtime.exec()`. This help in separating the classpath from the pipeline runner from the classpath of the pipeline itself, avoiding dependency version collisions.
For this to work, the `jcore-pipeline-runner-cpe` JAR needs to reside on the classpath of pipeline runner or in the same directory as the pipeline runner.
---

## v0.2.4 (25/09/2018)
Fixed a bug where pipelines were not usable by the pipeline-runner any more due to an ArrayOutOfBoundsException.
---

## v0.2.3 (25/09/2018)
NOTE: As of this release, this repository does not have its modules as subrepositories any more. The whole pipeline-modules project does now reside in this repository.

Then multiple components of the same description were added, e.g. multiple JSBDs, and then a reordering of JSBDs was issued by the user, other JSBDs could get lost or replaced. This has been fixed.

Also, there is now a warning message for repeated external resource names. Those won't be handled well by UIMA but rather only the first resource of a specific name will be used. Thus, it is now also possible to rename external resources.

It is now also possible to rename components. This is very useful when using multiple components of the same type with different configurations.
---

## v0.2.2 (20/09/2018)
Bugfix release.
---

## v0.2.1 (10/09/2018)

---

## v0.2 (20/07/2018)
This release features the possibility of the CLI pipeline builder to create completely new external resources dependencies for Analysis Engines (or consumers using an AnalysisEngineDescription).
The other components haven't seen much change.
---

## v0.1 (10/07/2018)
The JCoRe Pipeline Modules are a set of tools to facilitate the creation and running of NLP pipelines using UIMA components. They focus on the JCoRe component repositories but are also able to incorporate other components. For this purpose, each component needs to be described with a JSON file pointing out important information about a component such as its Maven coordinates, its name and the UIMA descriptors of the component. For examples of this JSON format, refer e.g. to [jcore-base](https://github.com/JULIELab/jcore-base/tree/2.3.0-SNAPSHOT).

The starting point for creating a pipeline using the tools offered here is the `jcore-pipeline-builder-cli`. There is also a graphical UI for pipeline building but it is not fully functional currently. It is also not clear if it will ever be.

Using the pipeline builder, components can be interactively selected, configured and the pipeline can be saved. Saving a pipeline creates a specific directory structure and the specified directory containing the UIMA descriptors and all JARs required to run the pipeline. The pipeline builder can also load existing pipelines for further editing.

The pipeline runner can then read such a created directory structure. The runner requires an XML configuration file specifying the location of the pipeline and other parameters such as the number of threads to run. To create such a configuration file, just call the pipeline runner and deliver a path to which a template configuration should be written. You will note that the template offers two possibilities to run pipelines, namely the CPE runner and the DUCC runner. Currently, only the CPE runner can be used. The DUCC runner will successfully commit a job to your DUCC cluster but unfortunately it will not successfully run. As of now, we don't know the reasons for this. So you should stick to the CPE runner for the time being.