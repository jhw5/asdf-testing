call mvn clean
call mvn package
net use x: \\192.168.0.87\nphrases_staging
copy target/nphrases.war x:/nphrases.war /y
net use x: /delete