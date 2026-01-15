install_jars:
	mkdir -p ./jars
	mvn -DoutputDirectory=./jars -f ./pom.xml dependency:copy-dependencies

build_binary:
	go build -o ./cmd/sample/sample ./cmd/sample

build_adv_binary:
	go build -o ./cmd/advanced/advanced_sample ./cmd/advanced

run: build_binary install_jars
	java -Dlogback.configurationFile=./logback.xml -cp "./jars/*" software.amazon.kinesis.multilang.MultiLangDaemon \
		./sample_kcl.properties

run_adv: build_adv_binary install_jars
	java -Dlogback.configurationFile=./logback.xml -cp "./jars/*" software.amazon.kinesis.multilang.MultiLangDaemon \
		./advanced_kcl.properties
