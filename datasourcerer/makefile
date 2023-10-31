NAME=datasourcerer
VERSION=0.1.0
OS_ARCH=linux_amd64
BINARY=${NAME}_${VERSION}_${OS_ARCH}


default: install

build:
	go build -o ${BINARY}

release:
	@# GOOS=darwin GOARCH=amd64 go build -o ./bin/${NAME}_${VERSION}_darwin_amd64
	@# GOOS=freebsd GOARCH=386 go build -o ./bin/${NAME}_${VERSION}_freebsd_386
	@# GOOS=freebsd GOARCH=amd64 go build -o ./bin/${NAME}_${VERSION}_freebsd_amd64
	@# GOOS=freebsd GOARCH=arm go build -o ./bin/${NAME}_${VERSION}_freebsd_arm
	@# GOOS=linux GOARCH=386 go build -o ./bin/${NAME}_${VERSION}_linux_386
	GOOS=linux GOARCH=amd64 go build -o ./bin/${NAME}_${VERSION}_linux_amd64
	@# GOOS=linux GOARCH=arm go build -o ./bin/${NAME}_${VERSION}_linux_arm
	@# GOOS=openbsd GOARCH=386 go build -o ./bin/${NAME}_${VERSION}_openbsd_386
	@# GOOS=openbsd GOARCH=amd64 go build -o ./bin/${NAME}_${VERSION}_openbsd_amd64
	@# GOOS=solaris GOARCH=amd64 go build -o ./bin/${NAME}_${VERSION}_solaris_amd64
	@# GOOS=windows GOARCH=386 go build -o ./bin/${NAME}_${VERSION}_windows_386
	@# GOOS=windows GOARCH=amd64 go build -o ./bin/${NAME}_${VERSION}_windows_amd64

install: build
	mv ${BINARY} /usr/local/bin/datasourcerer
