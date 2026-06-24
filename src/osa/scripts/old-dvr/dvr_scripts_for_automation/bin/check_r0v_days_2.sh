#!/usr/bin/env bash
set -euo pipefail

BASE="/fefs/onsite/data/lst-pipe/LSTN-01"
LOG="$BASE/R0V/log"

cd /local/home/lstanalyzer/DVR

for d in "$@"; do
  echo "==== $d ===="

  ./count_subruns.sh -t R0G -d "$d" > "R0G_${d}_count_subruns.txt"
  ./count_subruns.sh -t R0V -d "$d" > "R0V_${d}_count_subruns.txt"

  # 1) Comparar subruns
  if diff -u "R0G_${d}_count_subruns.txt" "R0V_${d}_count_subruns.txt" >/dev/null; then
    sr_status="SUBRUNS: OK (R0G = R0V)"
  else
    sr_status="SUBRUNS: DIFERENCIAS"
    diff -u "R0G_${d}_count_subruns.txt" "R0V_${d}_count_subruns.txt" || true
  fi

  # 2) Logs (si existen)
  if compgen -G "$LOG/$d/*.log" > /dev/null; then
    tot=$(ls "$LOG/$d"/*.log 2>/dev/null | wc -l)
    ok=$(grep -il "finished success" "$LOG/$d"/*.log 2>/dev/null | wc -l)
    echo "$sr_status  |  LOGS: $ok OK / $tot total"
    ugrep -L "finished success" "$LOG/$d"/*.log || true
  else
    echo "$sr_status  |  SIN LOGS"
  fi

  # 3) Chequeo por ficheros SIEMPRE (rápido)
  r0g_files=$(find "$BASE/R0G/$d" -type f -name "*Run*.fits.fz" 2>/dev/null | wc -l)
  r0v_files=$(find "$BASE/R0V/$d" -type f -name "*Run*.fits.fz" 2>/dev/null | wc -l)
  echo "FILES: R0G=$r0g_files  R0V=$r0v_files"

  if [[ "$r0g_files" -eq "$r0v_files" && "$r0v_files" -gt 0 ]]; then
    echo "VEREDICTO: OK (subruns/logs/files consistentes o sin logs pero completos)"
  else
    echo "VEREDICTO: REVISAR (desbalance de ficheros)"
  fi
done
