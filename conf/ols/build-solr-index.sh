#!/usr/local/bin/bash

java -Xmx2g -Dspring.profiles.active=vfb -Dols.home=/nfs/spot/data/dev/vfb -jar OLS-master/ols-apps/ols-solr-app/target/ols-solr-app.jar
java -Xmx2g -Dspring.profiles.active=fbdv -Dols.home=/nfs/spot/data/dev/vfb -jar OLS-master/ols-apps/ols-solr-app/target/ols-solr-app.jar
