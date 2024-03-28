# Hadoop


Commande:
hdfs dfsadmin -report
 start-dfs.sh
 start-yarn.sh
 hadoop fs -put /vagrant/tp/1/nomfichier
 cd /vagrant/tp/1/nomdossier
 javac ClasseJava.java
 mkdir -p nom_package
 mv NomClasseCompilé*.class nom_package
 jar -cvf hadoop-NomJAR.jar -C . com
 rm -rf com
 hadoop jar NomJAR.jar nom_package.HadoopAnagram nom_fichier nom_fichier_résultat

 hadoop fs -cat nom_fichier_résultat/*
