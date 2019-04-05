# Big-Data-Analysis-with-Scala-and-Spark

# Week 1 
Nous utiliserons les données en texte intégral de Wikipedia pour produire une mesure rudimentaire
de la popularité d'un langage de programmation, afin de voir si notre classement basé sur Wikipedia a un lien quelconque   
avec le populaire classement Red Monk.  
Data => http://alaska.epfl.ch/~dockermoocs/bigdata/wikipedia.dat à copier dans src/main/resources/wikipedia.  

# Week 2    
L’objectif général de cette mission est de mettre en œuvre un algorithme k-means distribué qui regroupe les publications sur la plate-forme de questions-réponses StackOverflow, très populaire, en fonction de leur score. De plus, cette classification doit être exécutée en parallèle pour différents langages de programmation et les résultats doivent être comparés.  
 
StackOverflow est une source importante de documentation. Cependant, différentes réponses fournies par les utilisateurs peuvent avoir des notations très différentes (basées sur les votes des utilisateurs) en fonction de leur valeur perçue. Nous aimerions donc examiner la répartition des questions et leurs réponses. Par exemple, combien de réponses très bien notées les utilisateurs de StackOverflow publient-elles et quel est leur score? Existe-t-il de grandes différences entre les réponses mieux notées et les réponses moins bien notées?  
http://alaska.epfl.ch/~dockermoocs/bigdata/stackoverflow.csv à copier dans src/main/resources/stackoverflow

La partie k-mean étant déjà développé. Il fallait donc preparer les données, les parser et preparer quelques fonctions qui vont servir aux calculs des kmeans. Ce projet à pour but de mettre en pratique le scala/spark dans un écosysteme de machine learning.

# Week 3 (En réalisation)   

Le jeu de données est fourni par Kaggle et est documenté ici:

https://www.kaggle.com/bls/american-time-use-survey
http://alaska.epfl.ch/~dockermoocs/bigdata/timeusage.zip

Notre objectif est d'identifier trois groupes d'activités:  

besoins primaires (dormir et manger),  
travail,  
autre (loisir).  
Et ensuite, observez comment les gens répartissent leur temps entre ces trois types d’activités et si on peut voir des différences entre hommes et femmes, actifs et chômeurs, et jeunes (moins de 22 ans), actifs (entre 22 et 55 ans). vieux) et les aînés.  

À la fin de la mission, nous pourrons répondre aux questions suivantes basées sur l'ensemble de données:  

combien de temps consacrons-nous aux besoins primaires par rapport aux autres activités?  
les femmes et les hommes consacrent-ils le même temps au travail?  
le temps consacré aux besoins primaires change-t-il avec l'âge?  
combien de temps les personnes employées consacrent-elles aux loisirs par rapport aux personnes sans emploi?  
