
WORKSPACE = $(shell echo "${PWD}")
ROCKSDB_DST = $(WORKSPACE)/opt/rocksdb
ROCKSDB_SRC = $(WORKSPACE)/../3rd-party/rocksdb
STORIKS_DST = $(WORKSPACE)/opt/storiks
STORIKS_SRC = $(WORKSPACE)/..
YCSB_SRC = $(WORKSPACE)/../3rd-party/YCSB
YCSB_DST = $(WORKSPACE)/opt/YCSB
PERFMON_DST = $(WORKSPACE)/opt/performancemonitor
PERFMON_SRC = $(WORKSPACE)/../3rd-party/performancemonitor

DOCKER_IMAGE = $(shell ../storiksctl config --get DOCKER_IMAGE)

all:
	@+make opt
	@make docker

docker:
	$(info Creating docker image "$(DOCKER_IMAGE)")
	@docker build -t "$(DOCKER_IMAGE)" --compress .

opt:
	@mkdir opt || true
	@+make storiks
	@+make rocksdb
	@+make ycsb
	@+make performancemonitor

rocksdb: storiks
	@+cd "$(STORIKS_SRC)" && make rocksdb
	@+cd "$(STORIKS_SRC)" && make rocksdb-java
	$(info Install rocksdb from $(ROCKSDB_SRC) to $(ROCKSDB_DST))
	@+cd "$(STORIKS_SRC)" && make ROCKSDB_PREFIX="$(ROCKSDB_DST)" rocksdb-install
	@mkdir -p "$(ROCKSDB_DST)"/bin || true
	@find "$(ROCKSDB_SRC)" -maxdepth 1 -type f -perm /u=x -exec cp {} "$(ROCKSDB_DST)/bin" \;
	@find "$(ROCKSDB_SRC)/tools" -maxdepth 1 -type f -perm /u=x -exec cp {} "$(ROCKSDB_DST)/bin" \;
	@find "$(ROCKSDB_DST)/bin" -maxdepth 1 -type f -name 'lib*' -delete
	@find "$(ROCKSDB_SRC)" -maxdepth 1 -type f -name 'librocksdb*' -exec cp {} "$(ROCKSDB_DST)/lib" \;
	@rm -f "$(ROCKSDB_DST)/lib/librocksdb.a"

storiks:
	@+cd "$(STORIKS_SRC)" && make bin
	@echo "Install storiks from $(STORIKS_SRC) to $(STORIKS_DST)"
	@rm -fr "$(STORIKS_DST)"
	@mkdir "$(STORIKS_DST)"
	@cp -a "$(STORIKS_SRC)/bin" "$(STORIKS_DST)"
	@rm -fr "$(STORIKS_DST)/bin/__pycache__"
	@cp "$(STORIKS_SRC)/build/access_time3" "$(STORIKS_DST)/bin"
	@cp "$(STORIKS_SRC)/build/storiks" "$(STORIKS_DST)/bin"
	@mkdir -p "$(STORIKS_DST)"/lib
	@cp -a "$(STORIKS_SRC)/lib/storiks" "$(STORIKS_DST)"/lib
	@rm -fr "$(STORIKS_DST)/lib/storiks/__pycache__"
	@cp -a "$(STORIKS_SRC)/examples" "$(STORIKS_DST)"
	@rm -fr "$(STORIKS_DST)/examples/.ipynb_checkpoints"

ycsb: rocksdb
	$(info Compile YCSB from $(YCSB_SRC))
	@+cd "$(STORIKS_SRC)" && make YCSB
	$(info Install YCSB from $(YCSB_SRC) to $(YCSB_DST))
	@rsync -ax "$(YCSB_SRC)/" "$(YCSB_DST)/"
	@cd "$(YCSB_DST)/rocksdb/target/dependency" && rm -f rocksdbjni-*.jar 
	@find "$(ROCKSDB_SRC)/java/target" -maxdepth 1 -type f -name 'rocksdbjni-*.jar' -exec cp {} "$(YCSB_DST)/rocksdb/target/dependency/rocksdbjni-linux64.jar" \;
	@test -z `fgrep YCSB_SLEEP $(YCSB_DST)/bin/ycsb.sh` && sed -i '17 a YCSB_SLEEP=$${YCSB_SLEEP:-0}; sleep "$$YCSB_SLEEP"' "$(YCSB_DST)/bin/ycsb.sh"

performancemonitor:
	@+cd "$(STORIKS_SRC)" && make performancemonitor
	$(info Install performancemonitor from $(PERFMON_SRC) to $(PERFMON_DST))
	@rm -fr "$(PERFMON_DST)"
	@rsync -ax "$(PERFMON_SRC)/" "$(PERFMON_DST)/"

clean:
	rm -fr opt
