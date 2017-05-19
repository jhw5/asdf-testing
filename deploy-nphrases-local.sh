CATALINA_HOME="/usr/local/Cellar/tomcat/8.0.9/libexec"
mvn clean
mvn package
cp ./tomcat-users.xml "$CATALINA_HOME/conf"
mv ./target/nphrases.war "$CATALINA_HOME/webapps"