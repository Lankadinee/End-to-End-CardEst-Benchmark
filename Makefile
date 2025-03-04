.PHONY: execute_permission apply_patch p_error build_docker stop_all_containers docker_run clean set_docker_permissions replace_file replace all create_stats_db copy_estimations create_db start_container test_one_file

IMAGE_NAME=ceb1
DATABASE_NAME=forest
CONTAINER_NAME=ce-benchmark-$(IMAGE_NAME)-$(DATABASE_NAME)
TEST_FILENAME=NA

all: execute_permission apply_patch build_docker init

test: execute_permission init test_db p_error stop_container

test_one: stop_all_containers start_container test_one_file stop_container

replace: replace_file build_docker

init: execute_permission stop_all_containers docker_run create_db copy_estimations set_docker_permissions

start_container:
	@docker start $(CONTAINER_NAME)
	echo "$(CONTAINER_NAME) Container started!"

stop_container:
	@docker stop $(CONTAINER_NAME)
	echo "$(CONTAINER_NAME) Container stopped!"

test_db: 
	@stdbuf -oL conda run -n cardest37 python -u scripts/py/send_query.py --database_name $(DATABASE_NAME) --container_name $(CONTAINER_NAME) 2>&1 | tee -a $(DATABASE_NAME)_test.log

test_one_file:
	@stdbuf -oL conda run -n cardest37 python -u scripts/py/send_query.py --database_name $(DATABASE_NAME) --container_name $(CONTAINER_NAME) --filename $(TEST_FILENAME) 2>&1 | tee -a $(DATABASE_NAME)_test.log

p_error:
	@conda run -n cardest37 python -u p_error_calculation.py --database_name $(DATABASE_NAME)
	mkdir -p scripts/plan_cost/$(DATABASE_NAME)/results
	mv scripts/plan_cost/$(DATABASE_NAME)/*.txt scripts/plan_cost/$(DATABASE_NAME)/results/

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
	echo "Starting docker"
	@docker rm -f $(CONTAINER_NAME) || true
	# @docker exec $(CONTAINER_NAME) mkdir -p /tmp/single_table_datasets
	# @docker cp single_table_datasets/${DATABASE_NAME} $(CONTAINER_NAME):/tmp/single_table_datasets
	@docker run -v $(shell pwd)/single_table_datasets/${DATABASE_NAME}:/tmp/single_table_datasets/${DATABASE_NAME}:ro -v $(shell pwd)/scripts:/tmp/scripts:ro --name $(CONTAINER_NAME) -p 5431:5432 -d $(IMAGE_NAME)
	echo "Docker is running"

stop_all_containers:
	@docker stop $(shell docker ps -a -q) || true

create_db:
	@sleep 2
	@./attach_and_run.sh $(DATABASE_NAME) $(CONTAINER_NAME)

set_docker_permissions:
	@docker exec --user root $(CONTAINER_NAME) chown -R postgres:postgres /var/lib/pgsql/13.1/data/
	@docker exec --user root $(CONTAINER_NAME) chmod -R 750 /var/lib/pgsql/13.1/data/

copy_estimations:
	echo "Copying estimations"
	# @docker cp workloads/$(DATABASE_NAME)/estimates/ $(CONTAINER_NAME):/var/lib/pgsql/13.1/data/
	@for file in workloads/$(DATABASE_NAME)/estimates/*.txt; do \
		echo "Copying $$file"; \
		docker cp "$$file" $(CONTAINER_NAME):/var/lib/pgsql/13.1/data/; \
	done

clean:
	@rm -rf postgresql-13.1
	@rm -rf dockerfile/postgres-13.1.tar.gz
	@docker rm -f ce-benchmark-$(IMAGE_NAME) || true
	@docker rmi $(IMAGE_NAME)
