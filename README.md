PyNOAA
======

This is a Python script for retrieving and processing the [NOAA][1] (The National Oceanic and Atmospheric Administration, a federal agency focused on the condition of the oceans and the atmosphere) weather dataset.

The NOAA dataset is divided in several folders, each of them contains a particular year starting from 1901. Inside a year folder we have a collection of tar files, compressed with bzip2. Each tar file contains a file for each weather station’s readings for the year, compressed with gzip. The script will download, uncompress and merge all year data into one single file per year. 

Data output can be retrieved in two formats, raw ASCII and ISH format. 

This script is useful for working with the examples described in the book ["Hadoop: The definitive Guide"][2]

    


  [1]: http://www.noaa.gov/
  [2]: http://hadoopbook.com/