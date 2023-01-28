# Parallel TopK Algorithm

CS3103 Operating System course project: Multi-threaded version TopK Algorithm.

Group 30: SEW Kin Hang, LOK Chun Pan

Report: CS3103_Group30_Project_Report.pdf

## Problem Description

In this project, we have to read large files of timestamps, then output the top K frequent timestamps grouped by hour. We developed two producer-consumer models for multi-file reading and parsing. More information can be found in the report.

## Getting Started

### Dependencies

* Operating system that supports POSIX Threads library

### Installing

* Download the whole repository

### Executing program

```
make clean
make build
python3 run_scripts.py
```