# flink_jobs package

## Overview

### jobs_runners
subpackage with scripts which run flink jobs
#### 1. publish_state
![publish_state job](png/publish_state.png)

### flink_functions
flink function (process, map) used by jobs

### flink_dataclasses
dataclasses used by jobs to store stage result, communicate with ellastic and kafka

### dead_letter_wrapper
wrapper which proceeds errors during flink function run

### errors
Exception classes used in package

### tests
tests

## Run tests
```
python setup.py pytest
```
