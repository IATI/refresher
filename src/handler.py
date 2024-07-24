import argparse

import library.clean as clean
import library.db as db
import library.flatten as flatten
import library.lakify as lakify
import library.refresher as refresher
import library.solrize as solrize
import library.validate as validate
from library.logger import getLogger

logger = getLogger("handler")


def main(args):
    try:
        if args.type == "refresh":
            db.migrateIfRequired()
            refresher.refresh()
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
    except Exception as e:
        logger.error("{} Failed. {}".format(args.type, str(e).strip()))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Refresh from IATI Registry")
    parser.add_argument("-t", "--type", dest="type", default="refresh", help="Trigger 'refresh' or 'validate'")
    parser.add_argument(
        "-e", "--errors", dest="errors", action="store_true", default=False, help="Attempt to download previous errors"
    )
    args = parser.parse_args()
    main(args)
