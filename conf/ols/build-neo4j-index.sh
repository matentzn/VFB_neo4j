#!/usr/local/bin/bash

java -Xmx2g -Dspring.profiles.active=vfb -Dols.home=/nfs/spot/data/dev/vfb -jar OLS-master/ols-apps/ols-neo4j-app/target/ols-neo4j-app.jar
java -Xmx2g -Dspring.profiles.active=fbdv -Dols.home=/nfs/spot/data/dev/vfb -jar OLS-master/ols-apps/ols-neo4j-app/target/ols-neo4j-app.jar
java -Xmx2g -Dspring.profiles.active=fbcv -Dols.home=/nfs/spot/data/dev/vfb -jar OLS-master/ols-apps/ols-neo4j-app/target/ols-neo4j-app.jar
java -Xmx2g -Dspring.profiles.active=fbbi -Dols.home=/nfs/spot/data/dev/vfb -jar OLS-master/ols-apps/ols-neo4j-app/target/ols-neo4j-app.jar
java -Xmx2g -Dspring.profiles.active=go -Dols.home=/nfs/spot/data/dev/vfb -jar OLS-master/ols-apps/ols-neo4j-app/target/ols-neo4j-app.jar


