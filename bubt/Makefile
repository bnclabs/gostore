build:
	go build

test:
	go test -v -race -test.run=. -timeout 4000s -test.bench=. -test.benchmem=true

coverage:
	go test -timeout 4000s -coverprofile=coverage.out
	go tool cover -html=coverage.out
	rm -rf coverage.out

clean:
	rm -rf coverage.out
