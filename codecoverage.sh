mvn clean install -fae jacoco:report
mvn jacoco:report-aggregate
mvnw jacoco:report-aggregate
open package/target/site/jacoco-aggregate/index.html
