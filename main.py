import argparse


def main():
    parser = argparse.ArgumentParser(description="Mekong jobs pipeline")
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")

    ohlcv_p = sub.add_parser("ohlcv-daily-ingest", help="Derive daily OHLCV bars from price snapshots in MinIO")
    ohlcv_p.add_argument("--date", metavar="YYYY-MM-DD", help="Target date (default: today)")
    tech_p = sub.add_parser("technical", help="Compute SMA/RSI/MACD/Bollinger Bands from OHLCV bars")
    tech_p.add_argument("--date", metavar="YYYY-MM-DD", help="Output partition date (default: today)")
    tech_p.add_argument("--full-recompute", action="store_true",
                        help="Ignore checkpoint and read all OHLCV history (slower but repairs state)")
    sub.add_parser("flink-alert", help="Flink DataStream job: real-time price alerts")
    sub.add_parser("volatility-burst", help="Flink DataStream job: sliding-window volatility burst detection")
    digest_p = sub.add_parser("digest", help="Daily digest of top gainers, losers, and volume leaders")
    digest_p.add_argument("--date", metavar="YYYY-MM-DD", help="Target date (default: today)")
    screener_p = sub.add_parser("screener", help="Weekly fundamental screener: P/E, ROE, EPS, D/E filter")
    screener_p.add_argument("--date", metavar="YYYY-MM-DD", help="Target date for ISO week (default: today)")

    args = parser.parse_args()

    if args.command == "ohlcv-daily-ingest":
        from jobs.batch.ohlcv_daily_ingest import run
        run(target_date=args.date)

    elif args.command == "technical":
        from jobs.batch.technical_job import run
        run(full_recompute=args.full_recompute, target_date=args.date)

    elif args.command == "flink-alert":
        from jobs.stream.price_alert_job import run
        run()

    elif args.command == "volatility-burst":
        from jobs.stream.volatility_burst_job import run
        run()

    elif args.command == "digest":
        from jobs.batch.digest_job import run
        run(target_date=args.date)

    elif args.command == "screener":
        from jobs.batch.screener_job import run
        run(target_date=args.date)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
