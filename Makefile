SUBDIRS := api bubt dict lib llrb log malloc

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

.PHONY: build $(SUBDIRS)
