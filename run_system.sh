#!/usr/bin/env bash
# =========================================================================
#  SMART PARKING MONITORING SYSTEM - TIMOR PLAZA DILI
#  Linux / macOS startup helper (same commands as run_system.bat).
#
#  Usage:  ./run_system.sh setup|data|db|producer|spark|dashboard|demo|test|perf
# =========================================================================
set -euo pipefail

cd "$(dirname "$0")"
PYTHON="python3"
[ -x "venv/bin/python" ] && PYTHON="venv/bin/python"

usage() {
    cat <<'EOF'
TIMOR PLAZA - SMART PARKING MONITORING SYSTEM

  ./run_system.sh setup       create venv and install dependencies
  ./run_system.sh data        generate the 10,000-record dataset
  ./run_system.sh db          create smart_parking and its schema
  ./run_system.sh producer    stream events into Kafka
  ./run_system.sh spark       start Spark Structured Streaming
  ./run_system.sh dashboard   open the Streamlit dashboard
  ./run_system.sh demo        fill the database without Kafka/Spark
  ./run_system.sh test        run the test suite
  ./run_system.sh perf        run the scalability test
EOF
}

case "${1:-}" in
    setup)
        python3 -m venv venv
        venv/bin/python -m pip install --upgrade pip
        venv/bin/python -m pip install -r requirements.txt
        echo "Done. Copy .env.example to .env and set POSTGRES_PASSWORD."
        ;;
    data)       $PYTHON data_generator/generate_dataset.py --records 10000 ;;
    db)         $PYTHON database/database.py --init ;;
    producer)   $PYTHON producer/kafka_producer.py --create-topic --rate 5 ;;
    spark)      $PYTHON spark/spark_streaming.py ;;
    dashboard)  $PYTHON -m streamlit run dashboard/app.py ;;
    demo)       $PYTHON scripts/load_demo_data.py --truncate ;;
    test)       $PYTHON -m pytest -v ;;
    perf)       $PYTHON scripts/performance_test.py --sizes 10000 50000 100000 ;;
    *)          usage ;;
esac
