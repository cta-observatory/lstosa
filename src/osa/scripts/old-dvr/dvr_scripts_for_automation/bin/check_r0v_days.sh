## check_r0v_days.sh
#!/usr/bin/env bash
set -euo pipefail

BASE="/fefs/onsite/data/lst-pipe/LSTN-01"
LOG="$BASE/R0V/log"

cd /local/home/lstanalyzer/DVR

for d in "$@"; do
  ./count_subruns.sh -t R0G -d "$d" > "R0G_${d}_count_subruns.txt"
  ./count_subruns.sh -t R0V -d "$d" > "R0V_${d}_count_subruns.txt"

  echo "==== $d ===="

  # 1) Comparar subruns (independiente de logs)
  if diff -u "R0G_${d}_count_subruns.txt" "R0V_${d}_count_subruns.txt" >/dev/null; then
    sr_status="SUBRUNS: OK (R0G = R0V)"
  else
    sr_status="SUBRUNS: DIFERENCIAS"
    diff -u "R0G_${d}_count_subruns.txt" "R0V_${d}_count_subruns.txt" || true
  fi

  # 2) Validación con logs; si no hay, validar por ficheros
  if compgen -G "$LOG/$d/*.log" > /dev/null; then
    tot=$(ls "$LOG/$d"/*.log 2>/dev/null | wc -l)
    ok=$(grep -il "finished success" "$LOG/$d"/*.log 2>/dev/null | wc -l)
    echo "$sr_status  |  LOGS: $ok OK / $tot total"
    ugrep -L "finished success" "$LOG/$d"/*.log || true
  else
    # Sin logs → conteo real de archivos de datos
    # OJO: usar patrón que encuentre "…Run….fits.fz" en cualquier posición:
    r0g_files=$(find "$BASE/R0G/$d" -type f -name "*Run*.fits.fz" 2>/dev/null | wc -l)
    r0v_files=$(find "$BASE/R0V/$d" -type f -name "*Run*.fits.fz" 2>/dev/null | wc -l)
    echo "$sr_status  |  SIN LOGS: archivos R0G=$r0g_files  R0V=$r0v_files"
    if [[ "$r0g_files" -eq "$r0v_files" && "$r0v_files" -gt 0 ]]; then
      echo "VEREDICTO: OK (datos completos sin logs)"
    else
      echo "VEREDICTO: REVISAR (desbalance R0G/R0V o 0 ficheros)"
    fi
  fi
done
