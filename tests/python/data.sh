#!/bin/bash

/opt/spark/bin/spark-submit --master "local[1]" --name generatedata generate_data.py data/XY_doubles -r 1000
/opt/spark/bin/spark-submit --master "local[1]" --name generatedata generate_data.py data/XY_doubles -r 10000
/opt/spark/bin/spark-submit --master "local[1]" --name generatedata generate_data.py data/XY_doubles -r 100000
/opt/spark/bin/spark-submit --master "local[1]" --name generatedata generate_data.py data/XY_doubles -r 1000000
/opt/spark/bin/spark-submit --master "local[1]" --name generatedata generate_data.py data/XY_doubles -r 10000000
/opt/spark/bin/spark-submit --master "local[1]" --name generatedata generate_data.py data/XY_doubles -r 100000000
/opt/spark/bin/spark-submit --master "local[1]" --name generatedata generate_data.py data/XY_doubles -r 1000000000
