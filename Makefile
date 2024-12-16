.PHONY: execute_permission apply_patch build_docker docker_run clean replace_file replace all create_stats_db copy_estimations

IMAGE_NAME=cebv3

all: execute_permission apply_patch build_docker

replace: replace_file build_docker

init: execute_permission docker_run create_stats_db copy_estimations

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
	@cd dockerfile && docker build -t $(IMAGE_NAME) --network=host .
	@rm -rf postgres-13.1.tar.gz

docker_run:
	@docker rm -f ce-benchmark-$(IMAGE_NAME) || true
	@docker run -it --name ce-benchmark-$(IMAGE_NAME) -p 5431:5432 -d $(IMAGE_NAME) 

create_stats_db:
	@./attach_and_run.sh ce-benchmark-$(IMAGE_NAME)

copy_estimations:
	@docker cp workloads/stats_CEB/sub_plan_queries/estimates/* ce-benchmark-$(IMAGE_NAME):/var/lib/pgsql/13.1/data/

clean:
	@rm -rf postgresql-13.1
	@rm -rf dockerfile/postgres-13.1.tar.gz
	@docker rm -f ce-benchmark-$(IMAGE_NAME) || true
	@docker rmi $(IMAGE_NAME)