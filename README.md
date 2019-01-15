# Mater_thesis - University of Bern/Neuchâtel/Fribourg
A proposed method for unsupervised anomaly detection for a multivariate building dataset.

## Abstract:

Ubiquitous devices employed in building facilities are allowing us to acquire a diverse amount
of data relative to the internal systems of buildings. This is contributing to the growing awareness 
of the gap that exists between the desired performance of a building and its actual performance. 
Automated fault detection and diagnostic (AFDD) systems have been showed to be effective at detecting 
the root cause of performance problems. This master thesis is interested in finding motif cluster 
(typical patterns) and discord clusters (atypical/abnormal patterns), two types of patterns used 
by some AFDD approaches. Our approach attains to discover daily patterns in a multivariate fashion 
for a studied building dataset by using the Gaussian Hidden Markov models. The discovered motif cluster 
profiles define the typical performance of the building, while the discovered discord cluster profiles 
spot potential performance problems of the building. Three proposed models create a label data frame 
that summarize all the daily patterns in a table allowing the researcher to do further aggregation 
about the motif/discord cluster profiles. The proposed models where tested in a case study where the North-East and
South-West ventilation systems of the studied building were compared. The results provide
information about the pattern evolution across different seasons and years, as well as the dy-
namics between various variables. In addition, anomalous daily profiles were spotted as a
multivariate pattern in the North-East ventilation system, that demonstrate how powerful this
approach is. Finally, this approach had good feedback from the building experts and the po-
tential of our approach motivates further research.

## Proposed mehodology for finding typical/atypical patterns:
In general, the proposed methodology uses unsupervised machine learning for discovering the typical and/or unique patterns, then using Hierarchical Agglomerative Clustering (HAC) techniques, one determines the existing families of profiles (branchs of the HAC's dendogram) that exits in the investigated variable. Based on the result, one can determine wich clusters are typical (motifs) and which are atypical (discords). For example, in figure 5.8 the typical patterns are the profiles: 33, 1, 9, 4, 25, 17, 22, 11 and 27. In constrast, profile 29 differs from previous profiles, therefore this profile is considered as an atypical pattern. 

![General_methodology](https://github.com/Borreguin/Master_thesis/blob/master/static/img/Dendo_cluster_4.JPG)

This work uses a Gaussian Hidden Markov Model for finding the different patterns that exist in a time series. In this way, each profile is a group of days where the studied variable have a similar behavior. When the model is good trained (see Fig. 4.24 b) then the shape of each profile is well defined (there are not much outliers), while a bad trained model gives an undefined shape with several outliers (see Fig. 4.24 a).

![profile_example](https://github.com/Borreguin/Master_thesis/blob/master/static/img/C_profile_2.JPG)

Fig. 4.18, shows an hypothetical example of what the Gaussian Hidden Markov Model does. The time series is splitted in samples and once the final GaHMM model is trainned then each sample can be identified by a number, which is the ID profile. The result of this process is used as it was explained in above for creating the families of profiles, through the HAC´s techniques. 

![General_methodology](https://github.com/Borreguin/Master_thesis/blob/master/static/img/Hyp_example_find_clusters.JPG)

You can read more details of this work in the following link:
https://github.com/Borreguin/Master_thesis/blob/master/readme.pdf

or make questions to: rg.sanchez.a@gmail.com

