import argparse
import traceback

import sentry_sdk

import library.clean as clean
import library.db as db
import library.flatten as flatten
import library.lakify as lakify
import library.refresher as refresher
import library.solrize as solrize
import library.validate as validate
from constants.config import config
from library.logger import getLogger
from library.prometheus import initialise_prom_metrics_and_start_server

logger = getLogger("handler")


def main(args):
    try:
        initialise_sentry(args.type)

        initialise_prom_metrics(args.type)

        if args.type == "refresh":
            db.migrateIfRequired()
            refresher.refresh_publisher_and_dataset_info()
        elif args.type == "refreshloop":
            db.migrateIfRequired()
            refresher.service_loop()
        else:
            db.checkVersionMatch()

            if args.type == "reload":
                refresher.reload(args.errors)
            elif args.type == "safety_check":
                validate.safety_check()
            elif args.type == "validate":
                validate.validate()
            elif args.type == "copy_valid":
                clean.copy_valid()
            elif args.type == "clean_invalid":
                clean.clean_invalid()
            elif args.type == "flatten":
                flatten.main()
            elif args.type == "lakify":
                lakify.main()
            elif args.type == "solrize":
                solrize.main()
            elif args.type == "validateloop":
                validate.service_loop()
            elif args.type == "flattenloop":
                flatten.service_loop()
            elif args.type == "lakifyloop":
                lakify.service_loop()
            elif args.type == "solrizeloop":
                solrize.service_loop()
            elif args.type == "cleanloop":
                clean.service_loop()
            else:
                print(
                    "Type is required - either refresh, reload, safety_check, validate, "
                    "clean, flatten, lakify, or solrize - or their related service loop."
                )
    except Exception:
        logger.error("{} stage failed (full traceback follows)".format(args.type))
        logger.error(traceback.format_exc())


def initialise_prom_metrics(operation: str):
    if not operation.endswith("loop"):
        return

    logger.info("Starting prometheus metrics exporter...")

    if operation == "validateloop":
        container_conf_name = "VALIDATION"
    elif operation == "refreshloop":
        container_conf_name = "REFRESHER"
    else:
        container_conf_name = operation[:-4].upper()

    initialise_prom_metrics_and_start_server(
        config[container_conf_name]["PROM_METRIC_DEFS"], config[container_conf_name]["PROM_PORT"]  # type: ignore
    )


def initialise_sentry(operation: str):

    container_name = get_container_name_from_cli_operation(operation)

    sentry_sdk.init(
        dsn=config["SENTRY_DSN"],
        attach_stacktrace=True,
        send_default_pii=True,
        server_name=container_name,
    )


def get_container_name_from_cli_operation(operation: str):

    container_name = operation[:-4] if operation.endswith("loop") else operation

    return container_name


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Refresh from IATI Registry")
    parser.add_argument("-t", "--type", dest="type", default="refresh", help="Trigger 'refresh' or 'validate'")
    parser.add_argument(
        "-e", "--errors", dest="errors", action="store_true", default=False, help="Attempt to download previous errors"
    )
    args = parser.parse_args()
    main(args)
