The createMetaDescriptors.py is the original script to automatically
generate the UIMA component meta description from an existing Maven
project with UIMA component descriptor(s) in some subdirectory.
The original location of the script it the jcore-scripts directory
in the jcore-misc project. It is copied here to use it to add
arbitrary components to the local pipeline builder repository.

For this purpose, the pipeline builder will try to start the script
as an external process when adding a new component.
