TLS_PATH=${HOME}/.distributed-logger-tls/
.PHONY: init
init:
	mkdir -p ${TLS_PATH}
.PHONY: gencert
gencert:
	cfssl gencert \
		-initca config/tls/ca-csr.json | cfssljson -bare ca
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=config/tls/ca-config.json \
		-profile=server \
		config/tls/server-csr.json | cfssljson -bare server
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=config/tls/ca-config.json \
		-profile=client \
		config/tls/client-csr.json | cfssljson -bare client

.PHONY: comile
compile:
	protoc api/**/*.proto \
	--go_out=. --go_opt=paths=source_relative \
	--go-grpc_out=. --go-grpc_opt=paths=source_relative