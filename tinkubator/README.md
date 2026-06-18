= Tinkubator

The Tinkubator is a collection of experimental code and ideas that may not be ready for production, but are 
nevertheless valuable for exploration and expansion of the TinkerPop community. The concept of Tinkubator was developed
many years ago in the earliest years of the project, but it hasn't been in use for most of the TinkerPop 3.x arc. As
TinkerPop moves forward to 4.x and the opportunities that AI will bring, the Tinkubator offers a place to experiment
and explore new ideas at a faster pace with a lower barrier for acceptance.

== Current Projects

| Project   | Description                                                           | Stage |
|:----------|:----------------------------------------------------------------------|:-----:|
| Project A | This is a project that does something interesting for agentic memory. |   1   |
| Project B | This is a Gremlin Language Variant for RPG IV.                        |   2   |

== Project Stages

Tinkubator projects are categorized into three stages:

* 1: Early development where there is no formal release, high volatility in the code base (*experimental use only*).
* 2: Intermediate stage where there may be a preview release, but may not yet be considered stable (*experimental use only*).
* 3: Advanced stage where there is a preview release, documentation, and a stable API (*production capable but not officially supported*).

Stage 3 projects have likely reached a level of maturity where they could be considered for graduation from Tinkubator
and therefore officially supported by the community. Once they graduate, the project would get a non-preview release.

== Requirements for Tinkubator Projects

While the Tinkubator is meant to house projects in early stages of development, it is still intended to be a curated
set of projects that are on their way to production use. As such, there are some basic requirements for projects
that are to be considered for inclusion in the Tinkubator:

* It must have clear build instructions.
* If it is a GLV, it must pass the full TinkerPop test suite using our common patterns with Docker/Maven.
* It must have serviceable user documentation and clearly describe its position as experimental and part of Tinkubator.
* Its version number must reflect something early stage/experimental as idiomatic to TinkerPop and its programming
  language ecosystem.

While these are the minimum requirements, meeting them does not automatically mean a project is accepted. Project
maintainers retain the option to decline acceptance for any reason and may impose additional requirements.

