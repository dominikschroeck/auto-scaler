#!/bin/bash
mkdir build-target
cd ../benchmark_libraries

rm -rf target/*
sh ./build_and_install.sh
rm -rf target/*

cd ../benchmark_largestate/
mvn clean package
cp target/*.jar ../scripts/build-target
rm -rf target/*

cd ../kafka-producers
mvn clean package
cp target/*.jar ../scripts/build-target
rm -rf target/*

cd ../scripts

echo "Done"