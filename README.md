# Basket Leave Detector

## Introduction

An Apache Beam pipeline written in Python to detect abandoned baskets by customers in an E-Commerce shop

### Functionality
The pipeline reads one or more ```JSON``` files and output the result in a set of output ```JSON``` files.

### Install on Linux
```
pip3 install -r requirements.txt
```

### Usage
#### Run locally using DirectRunner with N workers 
```
python3 main.py \
--input <file path or wildcard> \
--output <output preffix>
--direct_num_workers <N>
```

### Test
```
python3 main.py \
--input 'test_data/input/basket*.json'
--output test_data/output/customers
--direct_num_workers 1