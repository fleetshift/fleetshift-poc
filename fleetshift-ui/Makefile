.PHONY: help image-build-mock-servers image-build-mock-ui-plugins image-build-gui image-build-all image-push

DEV_REGISTRY ?= quay.io/$(USER)
IMAGE_TAG ?= latest

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  %-25s %s\n", $$1, $$2}'

image-build-mock-servers: ## Build the mock-servers container image
	podman build -f Dockerfile.mock-servers -t $(DEV_REGISTRY)/fleetshift-mock-servers:$(IMAGE_TAG) .

image-build-mock-ui-plugins: ## Build the mock-ui-plugins container image
	podman build -f Dockerfile.mock-ui-plugins -t $(DEV_REGISTRY)/fleetshift-mock-ui-plugins:$(IMAGE_TAG) .

image-build-gui: ## Build the GUI container image
	podman build -f Dockerfile.gui -t $(DEV_REGISTRY)/fleetshift-gui:$(IMAGE_TAG) .

image-build-all: image-build-mock-servers image-build-mock-ui-plugins image-build-gui ## Build all UI container images

image-push: ## Push all UI container images
	podman push $(DEV_REGISTRY)/fleetshift-mock-servers:$(IMAGE_TAG)
	podman push $(DEV_REGISTRY)/fleetshift-mock-ui-plugins:$(IMAGE_TAG)
	podman push $(DEV_REGISTRY)/fleetshift-gui:$(IMAGE_TAG)
