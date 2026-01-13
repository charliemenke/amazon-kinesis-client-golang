install_jars:
	mkdir -p ./jars
	cd mvn -DoutputDirectory=./jars -f ../pom.xml dependency:copy-dependencies

build_binary:
	go build -o ./cmd/sample/sample ./cmd/sample

run: build_binary install_jars
	java -Dlogback.configurationFile=./logback.xml -cp "./jars/*" software.amazon.kinesis.multilang.MultiLangDaemon \
		./cmd/sample/sample.properties
