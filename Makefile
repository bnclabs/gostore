SUBDIRS := api bogn bubt flock lib llrb lsm malloc

build:
	go build
	@for dir in $(SUBDIRS); do \
		echo $$dir "..."; \
		$(MAKE) -C $$dir build; \
	done

test:
	go test
	@for dir in $(SUBDIRS); do \
		echo $$dir "..."; \
		$(MAKE) -C $$dir test; \
	done

bench:
	@for dir in $(SUBDIRS); do \
		echo $$dir "..."; \
		$(MAKE) -C $$dir bench; \
	done

coverage:
	@for dir in $(SUBDIRS); do \
		echo $$dir "..."; \
		$(MAKE) -C $$dir coverage; \
	done

clean:
	@for dir in $(SUBDIRS); do \
		echo $$dir "..."; \
		$(MAKE) -C $$dir clean; \
	done

vet:
	@go vet ./...

lint:
	@golint ./...

.PHONY: build $(SUBDIRS)
