# Architecture

Within our workers, every bit of Python logic that's run is technically an action. These can be explicit actions which are defined via an `@action` decorator or these can be implicit actions which are the bits of `Workflow.run` control flow that actually require a python interpreter to run.

Python Client Lib -> Rust Client Bridge -> DB
DB -> Rust Workflow Runner -> Python action runners

## python: client library

## rust: client bridge

## rust: workflow runner
