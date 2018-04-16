# JCoRe Pipeline Modules

This is a multi module repository and project offering capabilities to create and run JCoRe UIMA pipelines. It has two main parts, the JCoRe Pipeline Builder(s) and the JCoRe Pipeline runner.

## Introduction

The [Unstructured Information Management Architecture (UIMA)](https://uima.apache.org/) is a component-based framework for the automated analysis of human-understandable artifacts like natural language or pictures with the goal to induce a computer-understandable structor onto them.
The [JULIE Lab  UIMA  Component  Repository (JCoRe)](https://github.com/JULIELab/jcore-base) is a collection of UIMA components for the analytics of natural language text developed at the JULIE Lab at the Friedrich Schiller Universität in Jena, Germany. This project is meant to facilitate the usage of the components without a deep understanding of UIMA or programming.
However, since UIMA as well as JCoRe are complex systems, some conventions and mechanics are required to successfully employ the tools offered here. The basic building blocks of UIMA should be known as they are described in the [UIMA Tutoral and Developer's Guides](https://uima.apache.org/d/uimaj-2.10.2/tutorials_and_users_guides.html#ugr.tug.aae.getting_started), chapter 1. Also, users should be aware that JCoRe is split into the [jcore-base](https://github.com/JULIELab/jcore-base) and [jcore-projects](https://github.com/JULIELab/jcore-projects) repositories. Both repositories are integrated into the JCoRe Pipeline Builders but there are conceptual differences users should be familiar with. For more information on the ideas and conventions behind JCoRe, please refer to the [jcore-base documentation](https://github.com/JULIELab/jcore-base).
