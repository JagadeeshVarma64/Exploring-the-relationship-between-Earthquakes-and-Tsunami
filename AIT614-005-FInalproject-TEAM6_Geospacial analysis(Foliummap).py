#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 23 20:54:20 2024

@author: gadirajujagadeeshvarma
"""

# Loading the data into df1
df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/jgadiraj@gmu.edu/AIT614FinalData.csv")

#installing folium

pip install folium

# Folium Map

from pyspark.sql.functions import col, when, lit
import folium
import pandas as pd
from branca.colormap import LinearColormap

# Filtering out the rows with null or missing Latitude and Longitude values
df2 = df1.filter(col("Latitude").isNotNull() & col("Longitude").isNotNull())


df3 = df2.withColumn("color", when(col("Earthquake Magnitude") < 6.5, "green")
                                .when((col("Earthquake Magnitude") >= 6.5) & (col("Earthquake Magnitude") < 7.0), "yellow")
                                .when((col("Earthquake Magnitude") >= 7.0) & (col("Earthquake Magnitude") < 7.5), "orange")
                                .when((col("Earthquake Magnitude") >= 7.5) & (col("Earthquake Magnitude") < 8.0), "red")
                                .when(col("Earthquake Magnitude") >= 8.0, "black")
                                .otherwise("gray"))

pdf = df3.toPandas()

# Converting 'Earthquake Magnitude' to numeric for calculating range
pdf['Earthquake Magnitude'] = pd.to_numeric(pdf['Earthquake Magnitude'])


world_map = folium.Map()

# color scale for the magnitude
color_scale = LinearColormap(
    colors=['green', 'yellow', 'orange', 'red', 'black'],
    vmin=pdf['Earthquake Magnitude'].min(),
    vmax=pdf['Earthquake Magnitude'].max()
)

# Plotting the points on the map
for idx, row in pdf.iterrows():
    folium.CircleMarker([row['Latitude'], row['Longitude']],
                        radius=5,
                        color=color_scale(row['Earthquake Magnitude']),
                        fill=True,
                        fill_color=color_scale(row['Earthquake Magnitude']),
                        fill_opacity=0.7).add_to(world_map)


color_scale.add_to(world_map)



world_map
