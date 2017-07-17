SUBDIRS := api malloc dict lib bubt llrb

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

bench:
	@for dir in $(SUBDIRS); do \
		echo $$dir "..."; \
		$(MAKE) -C $$dir bench; \
	done

vet:
	@go vet ./...

lint:
	@golint ./...

.PHONY: build $(SUBDIRS)
