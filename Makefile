SUBDIRS := api bubt dict lib llrb log malloc

build:
	@for dir in $(SUBDIRS); do \
		echo $$dir "..."; \
		$(MAKE) -C $$dir build; \
	done

check:
	@echo "\ntrying go vet ...\n"
	@go vet ./...
	@echo "\ntrying golint ...\n"
	@golint ./...


test:
	@for dir in $(SUBDIRS); do \
		echo $$dir "..."; \
		$(MAKE) -C $$dir test; \
	done

.PHONY: build $(SUBDIRS)
