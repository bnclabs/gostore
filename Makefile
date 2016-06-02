SUBDIRS := api log malloc dict lib bubt llrb

build:
	@for dir in $(SUBDIRS); do \
		echo $$dir "..."; \
		$(MAKE) -C $$dir build; \
	done

test:
	@for dir in $(SUBDIRS); do \
		echo $$dir "..."; \
		$(MAKE) -C $$dir test; \
	done

vet:
	@go vet ./...

lint:
	@golint ./...

.PHONY: build $(SUBDIRS)
