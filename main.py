import argparse


def main():
    parser = argparse.ArgumentParser(description="Mekong jobs pipeline")
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")

    ohlcv_p = sub.add_parser("ohlcv-daily-ingest", help="Derive daily OHLCV bars from price snapshots in MinIO")
    ohlcv_p.add_argument("--date", metavar="YYYY-MM-DD", help="Target date (default: today)")
    sub.add_parser("technical", help="Compute SMA/RSI/MACD/Bollinger Bands from OHLCV bars")
    sub.add_parser("flink-alert", help="Flink DataStream job: real-time price alerts")

    args = parser.parse_args()

    if args.command == "ohlcv-daily-ingest":
        from jobs.batch.ohlcv_daily_ingest import run
        run(target_date=args.date)

    elif args.command == "technical":
        from jobs.batch.technical_job import run
        run()

    elif args.command == "flink-alert":
        from jobs.stream.price_alert_job import run
        run()

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
