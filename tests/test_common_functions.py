import pytest
import os
import logging
import argparse
import boto3
try:
    import mock
except ImportError:
    from unittest import mock

try:
    import pyspark
except:
    import findspark
    findspark.init('/Users/ashisprasad/homebrew/Cellar/apache-spark/3.1.1/libexec')
    import pyspark

from datetime import datetime, timedelta

from moto import mock_secretsmanager

import steps.spark.common.utils as utils
import steps.spark.common.spark_utils as spark_utils

logger = logging.getLogger(__name__)

def test_get_parameters(monkeypatch, caplog):

    monkeypatch.setattr("sys.argv", ["pytest",
                                    "--correlation_id", "kickstart_vacancy_analytical_dataset",
                                    "--job_name", "kickstart",
                                    "--module_name", "vacancy"])
    assert utils.get_parameters(logger) == argparse.Namespace(correlation_id="kickstart_vacancy_analytical_dataset",
                                            job_name="kickstart",
                                            module_name="vacancy",
                                            start_dt="",
                                            end_dt="",
                                            clean_up_flg="false",
                                            e2e_test_flg="false")

    monkeypatch.setattr("sys.argv", ["pytest",
                                     "--job_name", "kickstart",
                                     "--module_name", "vacancy"])
    assert utils.get_parameters(logger) == argparse.Namespace(correlation_id="",
                                                              job_name="kickstart",
                                                              module_name="vacancy",
                                                              start_dt="",
                                                              end_dt="",
                                                              clean_up_flg="false",
                                                              e2e_test_flg="false")

    monkeypatch.setattr("sys.argv", ["pytest"])
    assert utils.get_parameters(logger) == argparse.Namespace(correlation_id="",
                                                              job_name="kickstart",
                                                              module_name="",
                                                              start_dt="",
                                                              end_dt="",
                                                              clean_up_flg="false",
                                                              e2e_test_flg="false")

    monkeypatch.setattr("sys.argv", ["pytest",
                                     "--correlation_id", "kickstart_vacancy_analytical_dataset",
                                     "--job_name", "kickstart",
                                     "--module_name", "vacancy",
                                     "--start_dt", "2020-01-01",
                                     "--end_dt", "2020-01-01",
                                     "--clean_up_flg", "true",
                                     "--e2e_test_flg", "true"])
    assert utils.get_parameters(logger) == argparse.Namespace(correlation_id="kickstart_vacancy_analytical_dataset",
                                                              job_name="kickstart",
                                                              module_name="vacancy",
                                                              start_dt="2020-01-01",
                                                              end_dt="2020-01-01",
                                                              clean_up_flg="true",
                                                              e2e_test_flg="true")

    monkeypatch.setattr("sys.argv", ["pytest",
                                     "--correlation_id", "kickstart_vacancy_analytical_dataset",
                                     "--job_name", "kickstart",
                                     "--module_name", "vacancy",
                                     "--start_dt", "2020-01-01",
                                     "--end_dt", "2020-01-01",
                                     "--clean_up_flg", "true",
                                     "--e2e_test_flg", "true",
                                     "--unknown_parameter", "true"])
    assert utils.get_parameters(logger) == argparse.Namespace(correlation_id="kickstart_vacancy_analytical_dataset",
                                                              job_name="kickstart",
                                                              module_name="vacancy",
                                                              start_dt="2020-01-01",
                                                              end_dt="2020-01-01",
                                                              clean_up_flg="true",
                                                              e2e_test_flg="true")
    with caplog.at_level(logging.WARNING):
        utils.get_parameters(logger)
    assert "Found unknown parameters during runtime ['--unknown_parameter', 'true']\n" in caplog.text


@mock.patch("steps.spark.common.utils.get_last_process_dt", return_value="2020-01-01")
def test_update_runtime_args_to_config(monkeypatch, caplog, mock_config):
    config=mock_config
    args=argparse.Namespace(correlation_id="",
                            job_name="kickstart",
                            module_name="vacancy",
                            start_dt="2020-01-01",
                            end_dt="2020-01-01",
                            clean_up_flg="true",
                            e2e_test_flg="true")
    with pytest.raises(SystemExit) as pytest_wrapped_e, caplog.at_level(logging.ERROR):
        utils.update_runtime_args_to_config(logger, args, config)
    assert pytest_wrapped_e.type == SystemExit
    assert pytest_wrapped_e.value.code == -1
    assert "Failed to update the config file because of error: Correlation Id not passed during the run time this is required as part of this process \n" in caplog.text

    args=argparse.Namespace(correlation_id="kickstart_vacancy_analytical_dataset",
                            job_name="",
                            module_name="vacancy",
                            start_dt="2020-01-01",
                            end_dt="2020-01-01",
                            clean_up_flg="true",
                            e2e_test_flg="true")
    with caplog.at_level(logging.WARNING):
        utils.update_runtime_args_to_config(logger, args, config)
    assert "Job_Name Passed as blank, setting this variable to kickstart\n" in caplog.text
    assert config["job_name"] == "kickstart"

    args=argparse.Namespace(correlation_id="kickstart_vacancy_analytical_dataset",
                            job_name="kickstart",
                            module_name="",
                            start_dt="2020-01-01",
                            end_dt="2020-01-01",
                            clean_up_flg="true",
                            e2e_test_flg="true")
    with pytest.raises(SystemExit) as pytest_wrapped_e, caplog.at_level(logging.ERROR):
        utils.update_runtime_args_to_config(logger, args, config)
    assert pytest_wrapped_e.type == SystemExit
    assert pytest_wrapped_e.value.code == -1
    assert "Failed to update the config file because of error: No module name passed please set the value of module as vacancy, application or payment and re-submit the jobs \n" in caplog.text

    args=argparse.Namespace(correlation_id="kickstart_vacancy_analytical_dataset",
                            job_name="kickstart",
                            module_name="vacancy",
                            start_dt="",
                            end_dt="2020-01-01",
                            clean_up_flg="true",
                            e2e_test_flg="true")
    utils.update_runtime_args_to_config(logger, args, config)
    assert config["start_date"] == "2020-01-01"

    args=argparse.Namespace(correlation_id="kickstart_vacancy_analytical_dataset",
                            job_name="kickstart",
                            module_name="vacancy",
                            start_dt="2020-01-01",
                            end_dt="",
                            clean_up_flg="true",
                            e2e_test_flg="true")
    utils.update_runtime_args_to_config(logger, args, config)
    assert config["end_date"] == datetime.strftime(datetime.now(), "%Y-%m-%d")

    args=argparse.Namespace(correlation_id="kickstart_vacancy_analytical_dataset",
                            job_name="kickstart",
                            module_name="vacancy",
                            start_dt="2020-01-01",
                            end_dt="2020-01-01",
                            clean_up_flg="",
                            e2e_test_flg="true")
    with caplog.at_level(logging.WARNING):
        utils.update_runtime_args_to_config(logger, args, config)
    assert "clean_up_flg Passed as blank, setting this variable to False as default\n" in caplog.text
    assert config["clean_up_flag"] == "false"

    args=argparse.Namespace(correlation_id="kickstart_vacancy_analytical_dataset",
                            job_name="kickstart",
                            module_name="vacancy",
                            start_dt="2020-01-01",
                            end_dt="2020-01-01",
                            clean_up_flg="false",
                            e2e_test_flg="")
    with caplog.at_level(logging.WARNING):
        utils.update_runtime_args_to_config(logger, args, config)
    assert "clean_up_flg Passed as blank, setting this variable to False as default\n" in caplog.text
    assert config["e2e_test_flag"] == "false"

    args=argparse.Namespace(correlation_id="KICKSTART_UNIT_TEST",
                            job_name="KICKSTART",
                            module_name="VACANCY",
                            start_dt="2020-01-01",
                            end_dt="2020-01-01",
                            clean_up_flg="TRUE",
                            e2e_test_flg="FALSE")
    utils.update_runtime_args_to_config(logger, args, config)
    assert config["correlation_id"] == "kickstart_unit_test"
    assert config["job_name"] == "kickstart"
    assert config["module_name"] == "vacancy"
    assert config["start_date"] == "2020-01-01"
    assert config["end_date"] == "2020-01-01"
    assert config["clean_up_flag"] == "true"
    assert config["e2e_test_flag"] == "false"


def test_retrieve_secrets(monkeypatch):
    class MockSession:
        class Session:
            def client(self, service_name):
                class Client:
                    def get_secret_value(self, SecretId):
                        return {"SecretBinary": str.encode(SECRETS)}

                return Client()

    monkeypatch.setattr(boto3, "session", MockSession)
    assert generate_dataset_from_htme.retrieve_secrets(mock_args(), SNAPSHOT_TYPE_FULL) == ast.literal_eval(
        SECRETS
    )

