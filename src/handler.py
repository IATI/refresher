import argparse
import library.refresher as refresher
import library.validate as validate
import library.validate_activity_level as validate_activity_level
import library.validate_adhoc as validate_adhoc
import library.flatten as flatten
import library.lakify as lakify
import library.solrize as solrize
import library.db as db
from datetime import datetime, timedelta
from constants.config import config

def main(args):
    if args.type == "refresh":
        db.migrateIfRequired()
        refresher.refresh()
    elif args.type == "refreshloop":
        db.migrateIfRequired()
        refresher.service_loop()
    else:
        db.checkVersionMatch()

        if args.type == "reload":
            refresher.reload(
                args.errors
            )
        elif args.type == "validate":
            validate.main()
        elif args.type == "validate_activity_level":
            validate_activity_level.main()
        elif args.type == "adhocvalidate":
            validate_adhoc.main()
        elif args.type == "flatten":
            flatten.main()
        elif args.type == "lakify":
            lakify.main()
        elif args.type == "solrize":
            solrize.main()
        elif args.type == "validateloop":
            validate.service_loop()
        elif args.type == "adhocvalidateloop":
            validate_adhoc.service_loop()
        elif args.type == "flattenloop":
            flatten.service_loop()
        elif args.type == "lakifyloop":
            lakify.service_loop()           
        elif args.type == "solrizeloop":
            solrize.service_loop()
        else:
            print("Type is required - either refresh, reload, validate, validate_activity_level, flatten, lakify, or solrize - or their related service loop.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Refresh from IATI Registry')
    parser.add_argument('-t', '--type', dest='type', default="refresh", help="Trigger 'refresh' or 'validate'")
    parser.add_argument('-e', '--errors', dest='errors', action='store_true', default=False, help="Attempt to download previous errors")
    args = parser.parse_args()
    main(args)