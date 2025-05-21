import pytest
from dotenv import load_dotenv

import library.db as db
from constants.config import load_config_from_env
from tests.integration.utilities import empty_azurite_containers, get_db_connection_patched, truncate_db_tables


@pytest.fixture
def setup_and_teardown(request, mocker):

    load_dotenv(request.param, override=True)

    overridden_config = load_config_from_env()

    mocker.patch.dict("constants.config.config", overridden_config)
    mocker.patch.dict("library.db.config", overridden_config)
    mocker.patch.dict("library.refresher.config", overridden_config)
    mocker.patch("library.db.getDirectConnection", get_db_connection_patched)

    mocker.patch("library.refresher.addCore")
    mocker.patch("library.refresher.clean_containers_by_id")

    db.migrateIfRequired()

    truncate_db_tables(overridden_config)

    empty_azurite_containers(overridden_config)

    yield
