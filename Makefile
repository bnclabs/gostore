build:
	go build -tags dict

test:
	go test -v -race -tags dict -test.run=. -test.bench=. -test.benchmem=true

coverage:
	go test -tags dict -coverprofile=coverage.out
	go tool cover -html=coverage.out
	rm -rf coverage.out

clean:
	rm -rf coverage.out
