.PHONY: execute_permission apply_patch build_docker docker_run clean

IMAGE_NAME=cebv2

all: execute_permission apply_patch build_docker

replace: replace_file build_docker

execute_permission:
	@chmod u+x *.sh

apply_patch:
	@echo "Applying patch"
	@./benchmark_builder.sh

replace_file:
	@cp costsize-mod.c postgresql-13.1/src/backend/optimizer/path/costsize.c

build_docker:
	@tar cvf postgres-13.1.tar.gz postgresql-13.1
	@mv postgres-13.1.tar.gz dockerfile/
	@cd dockerfile && docker build -t cebv2 --network=host .
	@rm -rf postgres-13.1.tar.gz

docker_run:
	@sudo docker run -it --name ce-benchmark-$(IMAGE_NAME) -p 5431:5432 -d $(IMAGE_NAME) 

clean:
	@rm -rf postgresql-13.1
	@rm -rf dockerfile/postgres-13.1.tar.gz
	@docker rmi $(IMAGE_NAME)