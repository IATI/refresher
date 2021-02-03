import argparse
import library.refresher as refresher
import library.build as build
import library.validate as validate
import library.db as db
from datetime import datetime
from constants.config import config




def main(args):
    db.migrateIfRequired()

    if args.type == "refresh":
        refresher.refresh()
    if args.type == "reload":
        refresher.reload(
            args.errors
        )
    if args.type == "build": 
        build.main()
    if args.type == "validate":
        validate.main()
    if args.type == "dailyrun":
        now = datetime.now()
        run_time = 1440 - config.DAILY_SHUTDOWN_PERIOD_MINS
        shutdown_time = now + datetime.timedelta(minutes = run_time)

        refresher.refresh()
        refresher.reload()
        build.main(shutdown_time)
    else:
        print("Type is required - either refresh, reload or build, or dailyrun.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Refresh/Build from IATI Registry')
    parser.add_argument('-t', '--type', dest='type', default="refresh", help="Trigger 'refresh' or 'build'")
    parser.add_argument('-e', '--errors', dest='errors', action='store_true', default=False, help="Attempt to download previous errors")
    args = parser.parse_args()
    main(args)