.PHONY: build
build:
	CGO_ENABLED=1 go build -tags="linux sqlite_omit_load_extension osusergo netgo" -trimpath -ldflags="-extldflags=-static -s -w" -o bin/mssql2pg .
