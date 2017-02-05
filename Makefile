PROJECT = emqttc
PROJECT_DESCRIPTION = Erlang MQTT Client
PROJECT_VERSION = 1.0

DEPS = gen_logger
dep_gen_logger = git https://github.com/emqtt/gen_logger.git

EUNIT_OPTS = verbose

CT_SUITES = emqttc

COVER = true

include erlang.mk

app:: rebar.config

