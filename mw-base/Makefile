all:
	@echo "nothing is done when 'all' is done, try make help"
help:	## show this help
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##//'

VENV := $(shell if [ -z "${VENV}" ] ; then echo /local/venvs/p11-mw-base ; else echo ${VENV} ; fi)

.PHONY: venv
venv: ## create a virtualenv and install dependencies
venv: export V_DDPT_V=0.7.3
venv:
	virtualenv $(VENV)
	$(VENV)/bin/pip install --no-cache-dir -r requirements.txt -r requirements-dev.txt # --extra-index-url http://localhost:8080

.PHONY: qa
qa:	## launch QA tests
	$(VENV)/bin/flake8 src
	$(VENV)/bin/pytest src/
	$(VENV)/bin/coverage run --source=src/ -m pytest src/
	$(VENV)/bin/coverage html
	$(VENV)/bin/coverage report
	$(VENV)/bin/mypy src

.PHONY: pytest
pytest:	## launch tests
	$(VENV)/bin/pytest src/

.PHONY: mypy
mypy:	## launch QA tests mypy only
	$(VENV)/bin/mypy src

.PHONY: runs
runs: export PYTHONPATH=src
runs:	## run the grpc server
	$(VENV)/bin/python -m ddpestores.sfe_svr -p 8181 --db-server t-db-pgs -d estore_p11_main_test --s3-profile otvl-tests -b otvl-tests

.PHONY: runc1
runc1: export PYTHONPATH=src
runc1:	## run the grpc test client1
	$(VENV)/bin/python -m estorectest.estore_client -p 8181 -c cat-cli1 --drop 1

.PHONY: runc2
runc2: export PYTHONPATH=src
runc2:	## run the grpc test client2
	$(VENV)/bin/python -m estorectest.estore_client -p 8181 -c cat-cli2 --drop 0

.PHONY: runmap
runmap: export PYTHONPATH=src
runmap:	## run the mediawiki mapper
	$(VENV)/bin/python -m mediawiki.pamtasks.map_ -p 8181 --drop 1 --s3-profile otvl-tests --pack-url-num 0

.PHONY: runetl1
runetl1: export PYTHONPATH=src
runetl1:	## run the mediawiki mapper
	$(VENV)/bin/python -m mediawiki.pamtasks.etl -p 8181 --map-id 1 --s3-profile otvl-tests --pam-per-pack 0

.PHONY: runetl2
runetl2: export PYTHONPATH=src
runetl2:	## run the mediawiki mapper
	$(VENV)/bin/python -m mediawiki.pamtasks.etl -p 8181 --map-id 2 --s3-profile otvl-tests --pam-per-pack 0

.PHONY: runred
runred: export PYTHONPATH=src
runred:	## run the mediawiki mapper
	$(VENV)/bin/python -m mediawiki.pamtasks.reduce -p 8181 --map-num 2

.PHONY: runco
runco: export PYTHONPATH=src
runco:	## run the grpc test ope client
	$(VENV)/bin/python -m ddpestorec.sfe_ope_cli -p 8181 --ready
	$(VENV)/bin/python -m ddpestorec.sfe_ope_cli -p 8181 --version
	$(VENV)/bin/python -m ddpestorec.sfe_ope_cli -p 8181 --shutdown 2

.PHONY: docker
docker:	## builds first mw docker images
	docker build \
		--build-arg V_DDPT_V=$(V_DDPT_V) \
		--build-arg DEBPY_IMAGE=$(V_CTR_PFX)debpy:$(V_DEBPY_V) \
		--build-arg DDPESTOREC_DEV_IMAGE=$(V_CTR_PFX)ddpestorec-dev:$(V_DDPT_V) \
		--build-arg V_P11MWB_V=$(V_P11MWB_V) \
		-t $(V_CTR_PFX)mwpam-etl:$(V_P11MWB_V) \
		. -f docker/Dockerfile.mwpam-etl
	docker push $(V_CTR_PFX)mwpam-etl:$(V_P11MWB_V)
