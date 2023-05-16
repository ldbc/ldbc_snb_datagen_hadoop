#!/bin/bash

set -euo pipefail

mkdir -p ../datagen-graphs-regen

export ZSTD_NBTHREADS=`nproc`
export ZSTD_CLEVEL=12
export NUM_UPDATE_PARTITIONS=1

for SERIALIZER in CsvBasic CsvComposite CsvCompositeMergeForeign CsvMergeForeign Turtle; do
    echo SERIALIZER: ${SERIALIZER}
    
    for DATEFORMATTER in StringDateFormatter LongDateFormatter; do
        echo DATEFORMATTER: ${DATEFORMATTER}

        for SF in 0.1 0.3 1 3 10 30 100 300 1000; do
            echo SF: ${SF}

            # create configuration file
            echo > params.ini
            echo ldbc.snb.datagen.generator.scaleFactor:snb.interactive.${SF} >> params.ini
            echo ldbc.snb.datagen.serializer.dynamicActivitySerializer:ldbc.snb.datagen.serializer.snb.csv.dynamicserializer.activity.${SERIALIZER}DynamicActivitySerializer >> params.ini
            echo ldbc.snb.datagen.serializer.dynamicPersonSerializer:ldbc.snb.datagen.serializer.snb.csv.dynamicserializer.person.${SERIALIZER}DynamicPersonSerializer >> params.ini
            echo ldbc.snb.datagen.serializer.staticSerializer:ldbc.snb.datagen.serializer.snb.csv.staticserializer.${SERIALIZER}StaticSerializer >> params.ini
            echo ldbc.snb.datagen.serializer.dateFormatter:ldbc.snb.datagen.util.formatter.${DATEFORMATTER}
            echo ldbc.snb.datagen.serializer.numUpdatePartitions:${NUM_UPDATE_PARTITIONS} >> params.ini
            echo ldbc.snb.datagen.parametergenerator.parameters:false >> params.ini

            # run datagen
            ./run.sh

            # move the output to a separate directory
            mv social_network ../datagen-graphs-regen/social_network-sf${SF}-${SERIALIZER}-${DATEFORMATTER}
            mv params.ini ../datagen-graphs-regen/social_network-sf${SF}-${SERIALIZER}-${DATEFORMATTER}

            # compress the result directory using zstd
            tar --zstd -cvf ../datagen-graphs-regen/social_network-sf${SF}-${SERIALIZER}-${DATEFORMATTER}.tar.zst ../datagen-graphs-regen/social_network-sf${SF}-${SERIALIZER}-${DATEFORMATTER}/

            rm -rf ../datagen-graphs-regen/social_network-sf${SF}-${SERIALIZER}-${DATEFORMATTER}/
        done
    done
done
