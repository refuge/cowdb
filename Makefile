BASE_DIR = $(shell pwd)
SUPPORT_DIR=$(BASE_DIR)/support
ERLC ?= $(shell which erlc)
ESCRIPT ?= $(shell which escript)
ERL ?= $(shell which erl)
APP := cowdb
REBAR?= rebar

$(if $(ERLC),,$(warning "Warning: No Erlang found in your path, this will probably not work"))

$(if $(ESCRIPT),,$(warning "Warning: No escript found in your path, this will probably not work"))

.PHONY: deps doc

all: deps compile

dev: devbuild

compile:
	@$(REBAR) compile

deps:
	@$(REBAR) get-deps

doc: dev
	$(REBAR) -C rebar_dev.config doc skip_deps=true

cover: dev
	COVER=1 prove t/*.t
	@$(ERL) -detached -noshell -eval 'etap_report:create()' -s init stop

clean:
	@$(REBAR) clean
	@rm -f t/*.beam t/temp.*
	@rm -f doc/*.html doc/*.css doc/edoc-info doc/*.png

distclean: clean
	@$(REBAR) delete-deps
	@rm -rf deps

dialyzer: compile
	@dialyzer -Wno_return -c ebin



#
# TESTS
#
CBT_ETAP_DIR=$(BASE_DIR)/t
export CBT_ETAP_DIR


ERL_FLAGS=-pa $(BASE_DIR)/deps/*/ebin -pa $(BASE_DIR)/ebin -pa $(CBT_ETAP_DIR)
export ERL_FLAGS

test: devbuild testbuild
	prove $(CBT_ETAP_DIR)/*.t

verbose-test: devbuild testbuild
	echo $(ERL_FLAGS)
	prove -v $(CBT_ETAP_DIR)/*.t

testbuild: testclean
	@echo "==> init test environement"
	@$(ERLC) -v -o $(CBT_ETAP_DIR) $(CBT_ETAP_DIR)/etap.erl
	@$(ERLC) -v -o $(CBT_ETAP_DIR) $(CBT_ETAP_DIR)/test_util.erl

testclean:
	@rm -rf $(CBT_ETAP_DIR)/*.beam
	@rm -rf $(CBT_ETAP_DIR)/temp.*

# development
#
devclean:
	$(REBAR) -C rebar_dev.config clean

devbuild: devdeps
	$(REBAR) -C rebar_dev.config compile

devdeps:
	$(REBAR) -C rebar_dev.config get-deps



