# Project: Data Wrangling - Open Street Map (Boston)
## Data Analysis, Data Wrangling 

For brief review of the project see `P3_brief.ipynb` file and for more detail see `P3.ipynb`.

In addition you can find the coding in `P3_codes.py`.

### Overview

OpenStreetMap (OSM) is a collaborative project to create a free editable map of the world. The creation and growth of OSM has been motivated by restrictions on use or availability of map information across much of the world, and the advent of inexpensive portable satellite navigation devices.

While very useful, OSM data can be quite messy at times. In this project, I will walk you through the cleaning process of the data, storing data in the CSV format and analyzing it via SQL queries. For the purpose of this study, I chose one of my favorite cities in united states, Boston. The XML file (414 MB) has been downloaded from [Map Zen](https://mapzen.com/data/metro-extracts/metro/boston_massachusetts/) website.

### Install

This project requires **Python 2.7** and the following Python libraries installed:

- [NumPy](http://www.numpy.org/)
- [Pandas](http://pandas.pydata.org)
- [matplotlib](http://matplotlib.org/)
- [scikit-learn](http://scikit-learn.org/stable/)

You will also need to have software installed to run and execute a [Jupyter Notebook](http://ipython.org/notebook.html)

If you do not have Python installed yet, it is highly recommended that you install the [Anaconda](http://continuum.io/downloads) distribution of Python, which already has the above packages and more included. Make sure that you select the Python 2.7 installer and not the Python 3.x installer. 

### Code

The code is provided in the `P3_codes.py` file and more comprehensively in `P3_brief.ipynb` notebook file. 

### Run

In a terminal or command window, navigate to the top-level project directory `Data-Wrangling--Open-Street-Map-Boston-XML/` (that contains this README) and run one of the following commands:

```bash
ipython notebook P3.ipynb
```  
or
```bash
jupyter notebook P3.ipynb
```

This will open the Jupyter Notebook software and project file in your browser.

## File List

1) boston_massachusetts_sample.db

2) boston_massachusetts_sample.osm

3) nodes.zip

4) nodes_tags.zip

5) ways.zip

6) ways_nodes.zip

7) ways_tags.zip

8) schema.py


