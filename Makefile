.PHONY: build-bqdispatch

build-bqdispatch-cli:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bqdispatch-cli ./dispatch/cli/
	docker build --platform linux/amd64 -f Dockerfile.bqdispatch-cli -t bqdispatch-cli .
	rm bqdispatch-cli
