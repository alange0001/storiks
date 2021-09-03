
WORKSPACE = $(shell echo "${PWD}")
ROCKSDB_DST = $(WORKSPACE)/opt/rocksdb
ROCKSDB_SRC = $(WORKSPACE)/../3rd-party/rocksdb
ROCKSDB_TEST_DST = $(WORKSPACE)/opt/rocksdb_test
ROCKSDB_TEST_SRC = $(WORKSPACE)/..
YCSB_SRC = $(WORKSPACE)/../3rd-party/YCSB
YCSB_DST = $(WORKSPACE)/opt/YCSB

DOCKER_IMG ?= "alange0001/rocksdb_test"
DOCKER_TAG ?= "latest"

all: docker

docker: opt
	$(info Creating docker image)
	@docker build -t "$(DOCKER_IMG):$(DOCKER_TAG)" --compress .

opt: rocksdb rocksdb_test ycsb

rocksdb-build:
	$(info Install rocksdb from $(ROCKSDB_SRC) to $(ROCKSDB_DST))
	@+cd "$(WORKSPACE)/.." && make ROCKSDB_PREFIX="$(ROCKSDB_DST)" rocksdb-install

rocksdb: rocksdb-build
	@mkdir -p "$(ROCKSDB_DST)"/bin || true
	@find "$(ROCKSDB_SRC)" -maxdepth 1 -type f -perm /u=x -exec cp {} "$(ROCKSDB_DST)/bin" \;
	@find "$(ROCKSDB_SRC)/tools" -maxdepth 1 -type f -perm /u=x -exec cp {} "$(ROCKSDB_DST)/bin" \;
	@find "$(ROCKSDB_DST)/bin" -maxdepth 1 -type f -name 'lib*' -delete
	@find "$(ROCKSDB_SRC)" -maxdepth 1 -type f -name 'librocksdb*' -exec cp {} "$(ROCKSDB_DST)/lib" \;
	@rm -f "$(ROCKSDB_DST)/lib/librocksdb.a"

rocksdb_test:
	@echo "Install rocksdb_test from $(ROCKSDB_TEST_SRC) to $(ROCKSDB_TEST_DST)"
	@mkdir -p "$(ROCKSDB_TEST_DST)"/bin || true
	@mkdir -p "$(ROCKSDB_TEST_DST)"/files || true
	@find "$(ROCKSDB_TEST_SRC)/files" -maxdepth 1 -type f -name '*.options' -exec cp {} "$(ROCKSDB_TEST_DST)/files" \;
	@cp "$(ROCKSDB_TEST_SRC)/build/access_time3" "$(ROCKSDB_TEST_DST)/bin"

ycsb-build:
	$(info Compile YCSB from $(YCSB_SRC))
	@+cd "$(WORKSPACE)/.." && make YCSB

ycsb: ycsb-build rocksdb
	$(info Install YCSB from $(YCSB_SRC) to $(YCSB_DST))
	@rsync -ax "$(YCSB_SRC)/" "$(YCSB_DST)/"
	@cd "$(YCSB_DST)/rocksdb/target/dependency" && rm -f rocksdbjni-*.jar 
	@find "$(ROCKSDB_SRC)/java/target" -maxdepth 1 -type f -name 'rocksdbjni-*.jar' -exec cp {} "$(YCSB_DST)/rocksdb/target/dependency/rocksdbjni-linux64.jar" \;
	@test -z `fgrep YCSB_SLEEP $(YCSB_DST)/bin/ycsb.sh` && sed -i '17 a YCSB_SLEEP=$${YCSB_SLEEP:-0}; sleep "$$YCSB_SLEEP"' "$(YCSB_DST)/bin/ycsb.sh"

clean:
	rm -fr opt
