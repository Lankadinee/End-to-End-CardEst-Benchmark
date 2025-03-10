.PHONY: execute_permission apply_patch p_error build_docker stop_all_containers docker_run clean set_docker_permissions \
replace_file replace all copy_estimations create_db start_container test_one_file test_experiment

IMAGE_NAME=ceb
DATABASE_NAME=custom
CONTAINER_NAME=ce-benchmark-$(IMAGE_NAME)-$(DATABASE_NAME)
TEST_FILENAME=custom_estimates_exp.txt

all: execute_permission apply_patch build_docker init

test: execute_permission init create_venv test_db p_error stop_container

test_one: stop_all_containers start_container create_venv test_one_file stop_container

replace: replace_file build_docker

init: execute_permission stop_all_containers docker_run create_db copy_estimations set_docker_permissions

start_container:
	@docker start $(CONTAINER_NAME)
	echo "$(CONTAINER_NAME) Container started!"

stop_container:
	@docker stop $(CONTAINER_NAME)
	echo "$(CONTAINER_NAME) Container stopped!"

delete_container:
	@docker rm -f $(CONTAINER_NAME) || true
	echo "$(CONTAINER_NAME) Container deleted!"

create_venv:
	@uv sync

test_db: 
	uv run scripts/py/send_query.py --database_name $(DATABASE_NAME) --container_name $(CONTAINER_NAME) 2>&1 | tee -a $(DATABASE_NAME)_test.log

test_one_file:
	uv run scripts/py/send_query.py --database_name $(DATABASE_NAME) --container_name $(CONTAINER_NAME) --filename $(TEST_FILENAME) 2>&1 | tee -a $(DATABASE_NAME)_test.log

test_experiment:
	uv run scripts/py/send_query_test_one_query.py 2>&1 | tee $(DATABASE_NAME)_test.log

run_experiment: build_docker init reset_logs run_sample_query show_logs

run_sample_query:
	@export PGPASSWORD=postgres
	@docker cp row_estimate.txt $(CONTAINER_NAME):/var/lib/pgsql/13.1/data/row_estimate.txt
	@psql -d custom -h localhost -U postgres -p 5431 -c "SET max_parallel_workers_per_gather=0;EXPLAIN (FORMAT JSON)SELECT COUNT(*) FROM custom WHERE Value_1 <= 650;"

show_logs:
	@docker exec $(CONTAINER_NAME) cat /var/lib/pgsql/13.1/data/custom_log_file.txt

reset_logs:
	@docker exec $(CONTAINER_NAME) rm /var/lib/pgsql/13.1/data/custom_log_file.txt || true

p_error:
	uv run p_error_calculation.py --database_name $(DATABASE_NAME)
	mkdir -p scripts/plan_cost/$(DATABASE_NAME)/results
	mv scripts/plan_cost/$(DATABASE_NAME)/*.txt scripts/plan_cost/$(DATABASE_NAME)/results/

execute_permission:
	@chmod u+x *.sh

apply_patch:
	@echo "Applying patch"
	@./benchmark_builder.sh

apply_custom_patch:
	@wget https://ftp.postgresql.org/pub/source/v13.1/postgresql-13.1.tar.bz2
	@tar xvf postgresql-13.1.tar.bz2 && cd postgresql-13.1
	@bash custom_patch.sh apply

replace_file:
	@cp scripts/postgres_files/costsize.c postgresql-13.1/src/backend/optimizer/path/costsize.c

build_docker:
	@tar cvf postgres-13.1.tar.gz postgresql-13.1
	@mv postgres-13.1.tar.gz dockerfile/
	@cd dockerfile && docker build -t $(IMAGE_NAME) --network=host .
	@rm -rf postgres-13.1.tar.gz

docker_run:
	echo "Starting docker"
	@docker rm -f $(CONTAINER_NAME) || true
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
	@rm -rf postgresql-13.1 || true
	@rm postgresql-13.1.tar.bz2 || true
	@bash custom_patch.sh create
	@rm -rf dockerfile/postgres-13.1.tar.gz || true
	@docker rm -f $(CONTAINER_NAME) || true
	@docker rmi $(IMAGE_NAME) || true
	@rm *.log || true
